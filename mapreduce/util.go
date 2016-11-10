package mapreduce

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/mateuszdyminski/gomr/service"
)

const (
	// MasterServiceName name of the master service in Consul.
	MasterServiceName = "master"

	// WorkerServiceName name of the worker service in Consul.
	WorkerServiceName = "worker"

	// GrpcConsulTag tag set for the worker to notify the clients that grpc API should be used.
	GrpcConsulTag = "grpc"

	// Separator separates key and value in intermediate files.
	Separator = ":::"

	// RetriesLimit number of retries of.
	RetriesLimit = 3

	// MapPhase name of the map phase.
	MapPhase = "map"

	// ReducePhase name of the reduce phase.
	ReducePhase = "reduce"
)

// fnvHash calculates hash from param string.
func fnvHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// reduceFileName returns the name of the intermediate file where the results of map tasks are stored.
func reduceFileName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeFileName returns the name of the output file of reduce task.
func mergeFileName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

// calculateFileLinesNumber calculates number of lines in particular file.
func calculateFileLinesNumber(f *os.File) (int, error) {
	totalLines := 0

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		totalLines++
	}

	if err := scanner.Err(); err != nil {
		return -1, err
	}

	return totalLines, nil
}

// changePaths changes the job paths: workDir and input to make them absolute if they are not.
func changePaths(job *service.MrJob) error {
	job.WorkDir = path.Join(job.Name, fmt.Sprintf("%d", time.Now().Unix()))

	pwd, err := os.Getwd()
	if err != nil {
		return err
	}

	if !path.IsAbs(job.Input) {
		job.Input = path.Join(pwd, job.Input)
	}

	return nil
}
