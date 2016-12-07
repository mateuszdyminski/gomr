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
		Name:           "Lotto",
		Input:          "/Users/md/workspace/go/src/github.com/mateuszdyminski/gomr/data/lotto-results",
		MapTasksNum:    5,
		ReduceTasksNum: 1,
		MrImplDir:      *mrImplDir,
		GoBinPath:      "/Users/md/workspace/gosrc/go/bin/go",
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

	log.Printf("status: %+v \n", summary.Status)
	log.Printf("result: %+v \n", summary.Result)
	log.Printf("duration: %+v \n", summary.Duration)
}
