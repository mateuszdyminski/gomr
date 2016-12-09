package api

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"time"

	consulApi "github.com/hashicorp/consul/api"
	"github.com/mateuszdyminski/gomr/service"
	"google.golang.org/grpc"
	"strings"
)

// MrClient holds information about the gomr client.
type MrClient struct {
	// consulClient client for consul to discover the master service
	consulClient *consulApi.Client
}

// JobResult holds info about the result of the submitted job.
type JobResult struct {
	Status   string
	Result   string
	Duration time.Duration
}

// NewMrClient constructs the instance of MrClient.
func NewMrClient(consulAddress string) (*MrClient, error) {
	// configure config
	cfg := consulApi.DefaultConfig()
	if consulAddress != "" {
		cfg.Address = consulAddress
	}

	// create consul client
	client, err := consulApi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("can't initialize Consul client. err: %v", err)
	}

	return &MrClient{
		consulClient: client,
	}, nil
}

// Submit submits the job to gomr framework. It blocks until the job will be done.
func (c *MrClient) Submit(job Job) (*JobResult, error) {
	// transform the job into the internal inplementation of MrJob
	mrJob, err := c.toMrJob(job)
	if err != nil {
		return nil, fmt.Errorf("can't prepare job for gomr. err: %v", err)
	}

	// find the master service in the consul
	masterClient, err := c.connectToMaster()
	if err != nil {
		return nil, err
	}

	// submit job
	statuses, err := masterClient.Submit(context.Background(), mrJob)
	if err != nil {
		return nil, fmt.Errorf("can't submit job(%v). err: %v", job, err)
	}

	// wait for the job result
	now := time.Now()
	lastStatus := service.Status_ERROR
	lastMsg := ""
	for {
		res, err := statuses.Recv()
		if err == io.EOF {
			log.Println("job done!")
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error while getting the job(%v) status. err: %v", job, err)
		}

		lastStatus = res.Status
		lastMsg = res.Msg
		log.Printf("serviceId: %v phase: %v status: %v\n", res.ServiceId, phaseToString(res.Phase), statusToString(res.Status))
	}

	return &JobResult{Duration: time.Since(now), Status: statusToString(lastStatus), Result: lastMsg}, nil
}

// statusToString transforms the internal job or phase status into the readable form.
func statusToString(status service.Status) string {
	switch status {
	case service.Status_DONE:
		return "phase done"
	case service.Status_ERROR:
		return "error"
	case service.Status_INPUT_LOADED:
		return "input files loaded"
	case service.Status_INTERMEDIATE_FILES_CREATED:
		return "intermediate files created"
	case service.Status_PLUGIN_LOADED:
		return "plugin loaded"
	case service.Status_ALL_DONE:
		return "job done"
	default:
		return "unknown"
	}
}

// statusToString transforms the internal phase name into the readable form.
func phaseToString(phase service.Phase) string {
	switch phase {
	case service.Phase_MAP:
		return "map"
	case service.Phase_REDUCE:
		return "reduce"
	default:
		return "unknown"
	}
}

// toMrJob transforms the external Job into the internal struct MrJob.
func (c *MrClient) toMrJob(job Job) (*service.MrJob, error) {
	mrJob := service.MrJob{}
	mrJob.Input = job.Input
	mrJob.MapTasksNum = job.MapTasksNum
	mrJob.ReduceTasksNum = job.ReduceTasksNum
	mrJob.Name = job.Name

	destDir := fmt.Sprintf("%s-%d", job.Name, time.Now().Unix())
	if _, err := exec.Command("cp", "-rf", job.MrImplDir, destDir).Output(); err != nil {
		return nil, err
	}
	defer os.RemoveAll(destDir)

	out, err := exec.Command("bash", "-c", job.GoBinPath+" version").Output()
	if err != nil {
		return nil, err
	}
	log.Printf("building plugin with go version: '%s' \n", strings.TrimRight(string(out), "\n"))

	outputFile := fmt.Sprintf("%s-%d.so", job.Name, time.Now().Unix())
	execString := fmt.Sprintf("cd %s && %s build -buildmode=plugin -o %s .", destDir, job.GoBinPath, outputFile)
	log.Printf("building plugin %s with command: '%s' \n", job.Name, execString)
	_, err = exec.Command("bash", "-c", execString).Output()
	if err != nil {
		return nil, err
	}

	log.Printf("plugin %s builded with success!\n", job.Name)

	f, err := os.Open(path.Join(destDir, outputFile))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	mrJob.MapReducePlugin = bytes

	return &mrJob, nil
}

// connectToMaster finds in consul and connects to the master service.
func (c *MrClient) connectToMaster() (service.MasterClient, error) {
	// query services which passes the health check
	services, _, err := c.consulClient.Health().Service("master", "grpc", true, nil)
	if err != nil {
		return nil, err
	}

	if len(services) == 0 {
		return nil, fmt.Errorf("master service was not found")
	}

	master := services[0].Service
	if len(services) > 1 {
		log.Printf("multiple master services found. choosing first. id: %s", master.ID)
	}

	if len(master.Tags) < 2 {
		return nil, fmt.Errorf("service (%s, %s) has no 2 tags - should contain at least 2 tags grpc and rpc address", master.ID, master.Service)
	}

	if _, _, err := net.SplitHostPort(master.Tags[1]); err != nil {
		return nil, fmt.Errorf("first element in tags should contain valid rpc address. err: %v", err)
	}

	// Connect to the server.
	conn, err := grpc.Dial(master.Tags[1], grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("fail to dial: %v", err)
	}

	log.Println("connected to master service!")
	return service.NewMasterClient(conn), nil
}
