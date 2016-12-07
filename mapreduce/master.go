package mapreduce

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/mateuszdyminski/gomr/service"
	"google.golang.org/grpc"
)

// Master holds information about the gomr Master service.
type Master struct {
	// Sync
	mutex sync.Mutex

	// Logger
	l *Logger

	// Consul
	consulClient *api.Client

	// workers
	workers map[string]*wrk // protect by mutex

	// Config
	name     string
	host     string
	httpPort int
	rpcPort  int

	// Work dir
	workDir string

	// gRPC
	grpcServer *grpc.Server

	// Quit chan
	quitWatcher chan int
	quit        chan os.Signal
}

// wrk holds information about the available gomr workers.
type wrk struct {
	id         string
	grpcConn   *grpc.ClientConn
	grpcClient service.MapReduceClient

	// TODO: add counter with errors counter - if the number exceeds some limit worker should be deleted from the pool
}

// NewMaster returns new instance of gomr Master service.
func NewMaster(host string, httpPort int, rpcPort int, workDir string, debug bool) (*Master, error) {
	// concat worker name
	serviceName := MasterServiceName

	// check work dir
	pwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("can't set work dir. err: %v", err)
	}

	if !path.IsAbs(workDir) {
		workDir = path.Join(pwd, workDir)
	}

	if err := os.MkdirAll(workDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("can't create work dir. err: %v", err)
	}

	// init logger
	log := NewLogger(fmt.Sprintf("[%s]", serviceName), debug)

	// create consul client
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("can't initialize Consul client. err: %v", err)
	}

	return &Master{
		l: log,

		consulClient: client,

		name:     serviceName,
		host:     host,
		httpPort: httpPort,
		rpcPort:  rpcPort,

		workDir: workDir,

		workers: make(map[string]*wrk),

		quitWatcher: make(chan int, 1),
		quit:        make(chan os.Signal, 1),
	}, nil
}

// Start starts the gomr master service - creates the health check server, register master service in consul and
// tries to connect to available warokers.
// Func waits until it gets signal{Kill, Interrupt} or Stop func is called.
func (m *Master) Start() error {
	// run http server
	m.startHealthCheckServer()

	// start master api - for mr clients
	if err := m.startGrpcServer(); err != nil {
		return err
	}

	// register in consul
	if err := m.registerService(); err != nil {
		return err
	}

	// connect with all workers
	if err := m.connectToWorkers(); err != nil {
		return err
	}

	// run workers watcher to add/remove workers
	go m.watchWorkers()

	// run until we get a signal
	m.waitUntil(os.Interrupt, os.Kill)
	return nil
}

// Stop stops the master service.
func (m *Master) Stop() error {
	m.quit <- os.Interrupt
	return nil
}

// waitUntil waits until the signal{Kill, Interrupt} has been sent to the service or Stop func is called.
func (m *Master) waitUntil(sigs ...os.Signal) {
	signal.Notify(m.quit, sigs...)
	<-m.quit

	m.quitWatcher <- 1

	m.mutex.Lock()
	for _, w := range m.workers {
		w.grpcConn.Close()
	}
	m.mutex.Unlock()

	// deregister consul service
	if err := m.consulClient.Agent().ServiceDeregister(m.name); err != nil {
		m.l.Error("error while deregistering the service %s from consul. err: %v", err)
	}

	m.l.Info("deregistered service %s in consul", m.name)
}

// registerService registers master service in consul.
func (m *Master) registerService() error {
	service := &api.AgentServiceRegistration{
		ID:      m.name,
		Name:    m.name,
		Port:    m.httpPort,
		Address: fmt.Sprintf("%s:%d", m.host, m.httpPort),
		Tags:    []string{GrpcConsulTag, fmt.Sprintf("%s:%d", m.host, m.rpcPort)},
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://%s:%d/health", m.host, m.httpPort),
			Interval: "1s",
			Timeout:  "1s",
		},
	}

	if err := m.consulClient.Agent().ServiceRegister(service); err != nil {
		return err
	}

	m.l.Info("registered service %s in consul!", m.name)
	return nil
}

// startHealthCheckServer starts the health check server for consul service discovery purpose.
func (m *Master) startHealthCheckServer() {
	// register endpoints and start http server
	go func() {
		// register consul health check endpoint
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "OK")
		})

		addr := fmt.Sprintf("%s:%d", m.host, m.httpPort)
		m.l.Info("starting http health check server on address: %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			m.l.Error("error while running health check http server. err: %v", err)
		}
	}()
}

// connectToWorkers tries to connect to all available workers.
func (m *Master) connectToWorkers() error {
	// query services which passes the health check
	services, _, err := m.consulClient.Health().Service(WorkerServiceName, GrpcConsulTag, true, nil)
	if err != nil {
		return err
	}

	m.addWorkers(services)

	return nil
}

// watchWorkers runs every 5 seconds to check whether there are new workers are available or removes them when are inactive.
func (m *Master) watchWorkers() {
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-m.quit:
			ticker.Stop()
			m.l.Info("quitting workers watcher!")
			return
		case <-ticker.C:
			m.l.Debug("starting looking for new workers!")

			// query services which passes the health check
			services, _, err := m.consulClient.Health().Service(WorkerServiceName, GrpcConsulTag, true, nil)
			if err != nil {
				m.l.Error("couldn't check available workers")
				continue
			}

			m.mutex.Lock()
			// check if all workers in the pool are still there
			for key := range m.workers {
				found := false
				for _, cService := range services {
					if cService.Service.ID == key {
						found = true
					}
				}

				if !found {
					m.l.Info("worker %s is not in the list of available services. removing!", key)
					// TODO: graceful shutdown the worker here. Probably quit channel should be added
					m.workers[key].grpcConn.Close()
					delete(m.workers, key)
				}
			}

			counter := m.addWorkers(services)
			m.mutex.Unlock()

			m.l.Debug("looking for new workers done! found %d new workers.", counter)
		}
	}
}

// addWorkers adds workers to the workers pool if any new workers appears in the services returned from consul.
func (m *Master) addWorkers(services []*api.ServiceEntry) int {
	counter := 0
	// add new services if any
	for _, cService := range services {
		if _, ok := m.workers[cService.Service.ID]; ok {
			m.l.Debug("worker %s is already in workers pool", cService.Service.ID)
			continue
		}

		if len(cService.Service.Tags) < 2 {
			m.l.Error("service (%s, %s) has no tags - should contain at least 2 tags with grpc and rpc address", cService.Service.ID, cService.Service.Service)
			continue
		}

		if _, _, err := net.SplitHostPort(cService.Service.Tags[1]); err != nil {
			m.l.Error("second element in tags should contain valid rpc address")
			continue
		}

		// Connect to the server.
		conn, err := grpc.Dial(cService.Service.Tags[1], grpc.WithInsecure())
		if err != nil {
			m.l.Error("fail to dial: %v", err)
			continue
		}

		wrk := &wrk{
			id:         cService.Service.ID,
			grpcConn:   conn,
			grpcClient: service.NewMapReduceClient(conn),
		}

		m.l.Info("worker (%s, %s) added to workers pool!", cService.Service.Service, cService.Service.ID)
		m.workers[cService.Service.ID] = wrk
		counter++
	}

	return counter
}

// startGrpcServer starts the grpc server for external API purpose - master-client communication.
func (m *Master) startGrpcServer() error {
	addr := fmt.Sprintf("%s:%d", m.host, m.rpcPort)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	m.grpcServer = grpc.NewServer(grpc.MaxMsgSize(1024*1024*20)) // 20mb
	service.RegisterMasterServer(m.grpcServer, m)
	go func() {
		m.l.Info("starting grpc server on address: %s", addr)
		if err := m.grpcServer.Serve(lis); err != nil {
			m.l.Error("error while running grpc server. err: %v", err)
		}
	}()

	return nil
}

// Submit exposes the submit for the gRPC gomr client. It submits the job, prepares inputs for map phase,
// coordinates the map and reduce phase and merge the outputs from the reduce phase.
// It blocks until the job is done.
func (m *Master) Submit(job *service.MrJob, stream service.Master_SubmitServer) error {
	// check paths to absolute
	if err := changePaths(job); err != nil {
		return nil
	}

	// prepare inputs for map phase
	files, err := m.makeInputs(job)
	if err != nil {
		return fmt.Errorf("can't create inputs for map phase. err: %v", err)
	}

	// start goroutine which pass all workers statuses to the gomr client
	notifications := make(chan *service.MrStatus)
	quit := make(chan bool)
	go m.notify(stream, notifications, quit)
	defer func() { quit <- true }()

	// run map phase
	if err := m.runMapPhase(*job, files, notifications); err != nil {
		return err
	}

	// run reduce phase
	if err := m.runReducePhase(*job, notifications); err != nil {
		return err
	}

	// merge the intermediate files into the one result
	resultFile, err := m.merge(job)
	if err != nil {
		return err
	}

	// send information that job has been finished with success
	if err := stream.Send(&service.MrStatus{ServiceId: m.name, Phase: service.Phase_REDUCE, Status: service.Status_ALL_DONE, Msg: resultFile}); err != nil {
		return err
	}

	return nil
}

// makeInputs splits the input file into intermediate files. NUmber of intermediate files is equal to number of map tasks.
func (m *Master) makeInputs(job *service.MrJob) ([]string, error) {
	m.l.Debug("creating inputs for map phase for job(%s) - original file(%s)", job.Name, job.Input)
	originalFile, err := os.Open(job.Input)
	if err != nil {
		return nil, err
	}
	defer originalFile.Close()

	// create file job workDir if it's not already there
	if err := os.MkdirAll(path.Join(m.workDir, job.WorkDir), os.ModePerm); err != nil {
		return nil, err
	}

	linesNumber, err := calculateFileLinesNumber(originalFile)
	if err != nil {
		return nil, err
	}

	m.l.Debug("total number of lines in original file(%d)", linesNumber)

	files := make(map[int]*os.File)
	var names []string
	for i := 0; i < int(job.MapTasksNum); i++ {
		filename := path.Join(m.workDir, job.WorkDir, fmt.Sprintf("mrinput-%d", i))

		file, err := os.Create(filename)
		if err != nil {
			return nil, err
		}

		files[i] = file
		names = append(names, filename)
	}

	i := 0
	originalFile.Seek(0, 0)
	scanner := bufio.NewScanner(originalFile)
	linesPerFile := linesNumber / int(job.MapTasksNum)
	for scanner.Scan() {
		fileIndex := i / linesPerFile
		if fileIndex > len(files)-1 {
			fileIndex = len(files) - 1
		}

		files[fileIndex].WriteString(scanner.Text())
		files[fileIndex].WriteString("\n")
		i++
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	m.l.Debug("closing files for map phase!")
	for _, f := range files {
		f.Close()
	}

	return names, nil
}

// merge combines the results of the many reduce jobs into a single output file.
func (m *Master) merge(job *service.MrJob) (string, error) {
	m.l.Debug("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < int(job.ReduceTasksNum); i++ {
		p := mergeFileName(job.Name, i)

		file, err := os.Open(path.Join(m.workDir, job.WorkDir, p))
		if err != nil {
			return "", err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			vals := strings.Split(line, " ")
			kvs[vals[0]] = strings.Join(vals[1:], " ")
		}

		if err := scanner.Err(); err != nil {
			return "", err
		}
	}

	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := path.Join(m.workDir, job.WorkDir, "mrtmp."+job.Name)
	file, err := os.Create(result)
	if err != nil {
		return "", err
	}
	defer file.Close()

	for _, k := range keys {
		if _, err := file.WriteString(fmt.Sprintf("%s: %s\n", k, kvs[k])); err != nil {
			return "", err
		}
	}

	return result, nil
}

// notify gets the task statuses from all workers and pass them to the gomr client.
func (m *Master) notify(stream service.Master_SubmitServer, notifications chan *service.MrStatus, quit chan bool) {
	for {
		select {
		case status := <-notifications:
			if err := stream.Send(status); err != nil {
				m.l.Error("can't send current status to the client. err: %v", err)
			}
		case <-quit:
			return
		}
	}
}
