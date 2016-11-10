package main

import (
	"flag"
	"log"

	"github.com/mateuszdyminski/gomr/mapreduce"
)

var (
	id       = flag.String("id", "1", "Worker id")
	host     = flag.String("host", "localhost", "Worker host")
	rpcPort  = flag.Int("rpc-port", 8101, "gRPC port - internode communication")
	httpPort = flag.Int("http-port", 8201, "Http port - health check status")
	debug    = flag.Bool("debug", false, "Whether to run worket with debug mode")
	workDir  = flag.String("work-dir", "results", "Work directory for intermediate files and result")
)

func main() {
	flag.Parse()

	wrk, err := mapreduce.NewWorker(*id, *host, *rpcPort, *httpPort, *workDir, *debug)
	if err != nil {
		log.Fatal(err)
	}

	if err := wrk.Start(); err != nil {
		log.Fatal(err)
	}
}
