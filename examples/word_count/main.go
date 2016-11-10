package main

import (
	"flag"
	"log"

	"github.com/mateuszdyminski/gomr/api"
)

var (
	mrImplDir     = flag.String("mrImplDir", "implementation", "A directory with the implementation of MapReduce interface")
	consulAddress = flag.String("consulAddress", "localhost:8500", "Address of Consul")
)

func main() {
	flag.Parse()

	job := api.Job{
		Name:           "WordCount",
		Input:          "/home/md/workspace/go/src/github.com/mateuszdyminski/gomr/data/chapter1",
		MapTasksNum:    5,
		ReduceTasksNum: 1,
		MrImplDir:      *mrImplDir,
	}

	client, err := api.NewMrClient(*consulAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("submitting job %s!\n", job.Name)

	summary, err := client.Submit(job)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Status: %+v \n", summary.Status)
	log.Printf("Result: %+v \n", summary.Result)
	log.Printf("Duration: %+v \n", summary.Duration)
}
