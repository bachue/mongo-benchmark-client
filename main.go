package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"time"

	mgo "gopkg.in/mgo.v2"
)

var (
	url            string
	dbName         string
	collectionName string
	batch          bool
	concurrency    int
	count          int
	command        string
	dataFilePath   string
)

func parseFlags() {
	flag.StringVar(&url, "url", "mongodb://localhost:27017", "MongoDB Connection String")
	flag.StringVar(&dbName, "db", "test-mongo", "MongoDB DB")
	flag.StringVar(&collectionName, "collection", "", "MongoDB Collection")
	flag.BoolVar(&batch, "batch", false, "Operate by batch or not")
	flag.IntVar(&concurrency, "concurrency", 1, "Concurrency")
	flag.IntVar(&count, "count", 1, "Count")
	flag.StringVar(&command, "command", "", "Command (`insert`, `query`)")
	flag.StringVar(&dataFilePath, "datafile", "records.txt", "The Datafile path to record or query")
	flag.Parse()

	if len(flag.Args()) > 0 {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	if command != "insert" && command != "query" {
		fmt.Fprintf(os.Stderr, "Usage: -command must be `insert` or `query`\n")
		os.Exit(1)
	}

	if len(collectionName) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: -collection must be specified\n")
		os.Exit(1)
	}

	if concurrency <= 0 {
		fmt.Fprintf(os.Stderr, "Usage: -concurrency must be greater than 0\n")
		os.Exit(1)
	}

	if count < concurrency {
		fmt.Fprintf(os.Stderr, "Usage: -count must be greater than or equal to -concurrency\n")
		os.Exit(1)
	}
}

func main() {
	parseFlags()
	session, err := mgo.Dial(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to mongodb: %s\n", err.Error())
		os.Exit(1)
	}
	defer session.Close()
	session.SetMode(mgo.Eventual, true)
	collection := session.DB(dbName).C(collectionName)

	var datafile *os.File
	if command == "insert" {
		datafile, err = os.OpenFile(dataFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	} else if command == "query" {
		datafile, err = os.OpenFile(dataFilePath, os.O_RDONLY, 0)
	} else {
		fmt.Fprintf(os.Stderr, "Invalid Command: %s\n", command)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open datafile: %s\n", err.Error())
		os.Exit(1)
	}
	defer datafile.Close()

	var (
		duration time.Duration
		succeed  int
		failed   int
	)

	if command == "insert" && batch {
		duration, err = insertByBatch(collection, datafile)
		if err != nil {
			fmt.Fprint(os.Stderr, "Execute error: %s", err.Error())
			os.Exit(1)
		}
		succeed = count
		failed = 0
	} else if command == "insert" {
		duration, succeed, failed = insertParallel(collection, datafile)
	} else if command == "query" {
		duration, succeed, failed = queryParallel(collection, datafile)
	} else {
		fmt.Fprintf(os.Stderr, "Invalid Command: %s\n", command)
		os.Exit(1)
	}

	if failed > 0 {
		fmt.Printf("Error percent: %f %%\n", float64(failed)*100.0/float64(count))
	}
	if succeed > 0 {
		fmt.Printf("Benchmark: %f q/s\n", float64(succeed*10e9)/float64(duration))
	}
}

func insertByBatch(collection *mgo.Collection, datafile *os.File) (duration time.Duration, err error) {
	array := make([]interface{}, count)
	for i := 0; i < count; i++ {
		array[i] = generateRecord()
	}
	beginTime := time.Now()
	err = collection.Insert(array...)
	endTime := time.Now()
	duration = endTime.Sub(beginTime)
	for i := 0; i < count; i++ {
		writeRecord(array[i].(map[string]string), datafile)
	}
	return
}

func insertParallel(collection *mgo.Collection, datafile *os.File) (duration time.Duration, succeed int, failed int) {
	array := make([]map[string]string, count)
	for i := 0; i < count; i++ {
		array[i] = generateRecord()
	}
	inputs, outputs, errors := prepareChannels()

	for i := 0; i < concurrency; i++ {
		go insertAsync(collection, inputs[i], outputs[i], errors[i], datafile)
	}

	for i := 0; i < count; i++ {
		inputs[i%concurrency] <- array[i]
	}
	for i := 0; i < concurrency; i++ {
		close(inputs[i])
	}
	duration, succeed, failed = waitForCases(outputs, errors)
	return
}

func insertAsync(collection *mgo.Collection, inputs <-chan map[string]string, outputs chan<- time.Duration, errors chan<- string, datafile *os.File) {
	defer close(outputs)
	defer close(errors)

	for {
		record, moreJob := <-inputs
		if moreJob {
			beginTime := time.Now()
			err := collection.Insert(record)
			endTime := time.Now()
			if err != nil {
				errors <- err.Error()
			} else {
				writeRecord(record, datafile)
				outputs <- endTime.Sub(beginTime)
			}
		} else {
			break
		}
	}
}

func queryParallel(collection *mgo.Collection, datafile *os.File) (duration time.Duration, succeed int, failed int) {
	array := make([]map[string]string, count)
	for i := 0; i < count; i++ {
		key, value, err := getLine(datafile)
		if err != nil {
			panic(err)
		}
		array[i] = make(map[string]string)
		array[i][key] = value
	}
	inputs, outputs, errors := prepareChannels()
	for i := 0; i < concurrency; i++ {
		go queryAsync(collection, inputs[i], outputs[i], errors[i])
	}
	for i := 0; i < count; i++ {
		inputs[i%concurrency] <- array[i]
	}
	for i := 0; i < concurrency; i++ {
		close(inputs[i])
	}
	duration, succeed, failed = waitForCases(outputs, errors)
	return
}

func prepareChannels() (inputs []chan map[string]string, outputs []chan time.Duration, errors []chan string) {
	tasksPerChannel := 1 + (count-1)/concurrency

	inputs = make([]chan map[string]string, concurrency)
	outputs = make([]chan time.Duration, concurrency)
	errors = make([]chan string, concurrency)
	for i := 0; i < concurrency; i++ {
		inputs[i] = make(chan map[string]string, tasksPerChannel)
		outputs[i] = make(chan time.Duration, concurrency)
		errors[i] = make(chan string, concurrency)
	}
	return
}

func queryAsync(collection *mgo.Collection, inputs <-chan map[string]string, outputs chan<- time.Duration, errors chan<- string) {
	defer close(outputs)
	defer close(errors)

	for {
		pair, moreJob := <-inputs
		if moreJob {
			beginTime := time.Now()
			count, err := collection.Find(pair).Count()
			endTime := time.Now()
			if err != nil {
				errors <- err.Error()
			} else {
				if count != 1 {
					fmt.Fprintf(os.Stderr, "Failed to find record.\n")
				}
				outputs <- endTime.Sub(beginTime)
			}
		} else {
			break
		}
	}
}

func waitForCases(outputs []chan time.Duration, errors []chan string) (durationSum time.Duration, succeed int, failed int) {
	var done int

	cases := make([]reflect.SelectCase, concurrency+concurrency)
	for i := 0; i < concurrency; i++ {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(outputs[i])}
	}
	for i := 0; i < concurrency; i++ {
		cases[i+concurrency] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(errors[i])}
	}

	durationSum = 0
	succeed = 0
	failed = 0
	done = 0

	for {
		chosen, value, ok := reflect.Select(cases)
		if ok {
			if chosen < concurrency {
				durationSum += time.Duration(value.Int())
				succeed += 1
			} else {
				fmt.Fprintf(os.Stderr, "%d: %s\n", chosen-concurrency, value.String())
				failed += 1
			}
		} else {
			done += 1
			if done >= count {
				return
			}
		}
	}
}

func getLine(datafile *os.File) (key string, value string, err error) {
	var (
		stat    os.FileInfo
		size    int64
		buf     []byte = make([]byte, 300)
		lineBuf []byte
		line    string
		results []string
	)
	stat, err = datafile.Stat()
	if err != nil {
		return
	}
	size = stat.Size()
	for {
		_, err = datafile.ReadAt(buf, rand.Int63n(size))
		if err != nil {
			return
		}
		bytesReader := bytes.NewReader(buf)
		bufReader := bufio.NewReader(bytesReader)
		lineBuf, _, err = bufReader.ReadLine()
		if err != nil {
			continue
		}
		line = string(lineBuf[:])
		results = strings.SplitN(line, ":", 3)
		if len(results) != 3 || len(results[2]) != 128 {
			lineBuf, _, err = bufReader.ReadLine()
			if err != nil {
				continue
			}
			line = string(lineBuf[:])
			results = strings.SplitN(line, ":", 3)
			if len(results) != 3 || len(results[2]) != 128 {
				continue
			}
		}
		key = "data" + results[1]
		value = results[2]
		return
	}
}
