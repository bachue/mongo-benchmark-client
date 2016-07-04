package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
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
	index          = mgo.Index{
		Name: "records_indexes",
		Key: []string{
			"data0", "data1", "data2", "data3", "data4", "data5", "data6", "data7", "data8", "data9",
			"data10", "data11", "data12", "data13", "data14", "data15", "data16", "data17", "data18", "data19",
		},
		Background: true,
	}
)

func parseFlags() {
	flag.StringVar(&url, "url", "mongodb://localhost:27017", "MongoDB Connection String")
	flag.StringVar(&dbName, "db", "test-mongo", "MongoDB DB")
	flag.StringVar(&collectionName, "collection", "", "MongoDB Collection")
	flag.BoolVar(&batch, "batch", false, "Operate by batch or not")
	flag.IntVar(&concurrency, "concurrency", 1, "Concurrency")
	flag.IntVar(&count, "count", 1, "Count")
	flag.StringVar(&command, "command", "", "Command (`insert`, `query`)")
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

	err = ensureIndex(collection)
	if err != nil {
		panic(err)
	}

	var datafile *os.File
	if command == "insert" {
		datafile, err = os.OpenFile("records", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	} else if command == "query" {
		datafile, err = os.OpenFile("records", os.O_RDONLY, 0)
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
		panic("not implemented")
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

func ensureIndex(collection *mgo.Collection) error {
	return collection.EnsureIndex(index)
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
		writeRecord(array[i].(*Record), datafile)
	}
	return
}

func insertParallel(collection *mgo.Collection, datafile *os.File) (duration time.Duration, succeed int, failed int) {
	array := make([]*Record, count)
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

func insertAsync(collection *mgo.Collection, inputs <-chan *Record, outputs chan<- time.Duration, errors chan<- string, datafile *os.File) {
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

func prepareChannels() (inputs []chan *Record, outputs []chan time.Duration, errors []chan string) {
	tasksPerChannel := 1 + (count-1)/concurrency

	inputs = make([]chan *Record, concurrency)
	outputs = make([]chan time.Duration, concurrency)
	errors = make([]chan string, concurrency)
	for i := 0; i < concurrency; i++ {
		inputs[i] = make(chan *Record, tasksPerChannel)
		outputs[i] = make(chan time.Duration, concurrency)
		errors[i] = make(chan string, concurrency)
	}
	return
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
