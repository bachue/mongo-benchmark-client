package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tuvistavie/securerandom"
	mgo "gopkg.in/mgo.v2"
)

type Record struct {
	data0  string
	data1  string
	data2  string
	data3  string
	data4  string
	data5  string
	data6  string
	data7  string
	data8  string
	data9  string
	data10 string
	data11 string
	data12 string
	data13 string
	data14 string
	data15 string
	data16 string
	data17 string
	data18 string
	data19 string
}

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

	var duration time.Duration

	if command == "insert" && batch {
		duration, err = insertByBatch(collection)
	} else if command == "insert" {
		panic("not implemented")
	} else if command == "query" {
		panic("not implemented")
	} else {
		fmt.Fprintf(os.Stderr, "Invalid Command: %s\n", command)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprint(os.Stderr, "Execute error: %s", err.Error())
		os.Exit(1)
	}

	fmt.Printf("Benchmark: %f q/s\n", float64(count*10e9)/float64(duration))
}

func ensureIndex(collection *mgo.Collection) error {
	return collection.EnsureIndex(index)
}

func insertByBatch(collection *mgo.Collection) (duration time.Duration, err error) {
	array := make([]interface{}, count)
	for i := 0; i < count; i++ {
		array[i] = generateRecord()
	}
	beginTime := time.Now()
	err = collection.Insert(array...)
	endTime := time.Now()
	duration = endTime.Sub(beginTime)
	return
}

func generateRecord() (record *Record) {
	record = &Record{
		data0:  generateRandomHex(64),
		data1:  generateRandomHex(64),
		data2:  generateRandomHex(64),
		data3:  generateRandomHex(64),
		data4:  generateRandomHex(64),
		data5:  generateRandomHex(64),
		data6:  generateRandomHex(64),
		data7:  generateRandomHex(64),
		data8:  generateRandomHex(64),
		data9:  generateRandomHex(64),
		data10: generateRandomHex(64),
		data11: generateRandomHex(64),
		data12: generateRandomHex(64),
		data13: generateRandomHex(64),
		data14: generateRandomHex(64),
		data15: generateRandomHex(64),
		data16: generateRandomHex(64),
		data17: generateRandomHex(64),
		data18: generateRandomHex(64),
		data19: generateRandomHex(64),
	}
	return
}

func generateRandomHex(n int) string {
	random, err := securerandom.Hex(n)
	if err != nil {
		panic(err)
	}
	return random
}
