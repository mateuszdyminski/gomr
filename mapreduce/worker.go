package mapreduce

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"

	"bufio"
	"crypto/md5"
	"github.com/hashicorp/consul/api"
	"github.com/mateuszdyminski/gomr/service"
	"google.golang.org/grpc"
	"path"
	"plugin"
	"reflect"
	"time"
)

// Wrk holds information about the gomr Worker service.
type Wrk struct {
	// l internal gomr logger
	l *Logger

	// consulClient client for consul service discovery
	consulClient *api.Client

	// grpcServer external gomr API server
	grpcServer *grpc.Server

	// workDir directory for intermediate files and job results
	workDir string

	// mrImplementations - loaded mapReduce implementations
	mrImplementations map[string]MapReduce

	// name of the worker service
	name string

	// id of the worker - for Consul purpose
	id string

	// host to run the worker on
	host string

	// rpcPort rpc port for external api available for gomr clients
	rpcPort int

	// httpPort http port for health checks server
	httpPort int

	// quit channel used when gomr worker is asked to stop
	quit chan os.Signal
}

// NewWorker returns new instance of gomr worker.
func NewWorker(id, host string, rpcPort, httpPort int, workDir string, debug bool) (*Wrk, error) {
	// init logger
	log := NewLogger(fmt.Sprintf("[%s-%s]", WorkerServiceName, id), debug)

	// check work dir
	pwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("can't set work dir. err: %v", err)
	}

	if !path.IsAbs(workDir) {
		workDir = path.Join(pwd, workDir)
	}

	// create consul client
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("can't initialize Consul client. err: %v", err)
	}

	return &Wrk{
		l:                 log,
		consulClient:      client,
		name:              WorkerServiceName,
		id:                fmt.Sprintf("%s-%s", WorkerServiceName, id),
		host:              host,
		rpcPort:           rpcPort,
		httpPort:          httpPort,
		mrImplementations: make(map[string]MapReduce),
		workDir:           workDir,
		quit:              make(chan os.Signal, 1),
	}, nil
}

// Start starts the gomr worker - creates the health check server and register workers service in consul.
// Func waits until it gets signal{Kill, Interrupt} or Stop func is called.
func (w *Wrk) Start() error {
	// run http server
	w.startHealthCheckServer()

	// register in consul
	if err := w.registerService(); err != nil {
		return err
	}

	// start external api
	if err := w.startGrpcServer(); err != nil {
		return err
	}

	// run until we get a signal
	w.waitUntil(os.Interrupt, os.Kill)
	return nil
}

// Stop stops the worker.
func (w *Wrk) Stop() error {
	w.quit <- os.Interrupt
	return nil
}

// waitUntil waits until it gets the particular signal in the quit channel.
func (w *Wrk) waitUntil(sigs ...os.Signal) {
	signal.Notify(w.quit, sigs...)
	<-w.quit

	// deregister consul service
	if err := w.consulClient.Agent().ServiceDeregister(w.id); err != nil {
		w.l.Error("error while deregistering the service %s from consul. err: %v", err)
	}

	w.l.Info("deregistered service %s in consul", w.name)
}

// startGrpcServer starts the grpc server for internal API purpose - master-worker communication.
func (w *Wrk) startGrpcServer() error {
	addr := fmt.Sprintf("%s:%d", w.host, w.rpcPort)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	w.grpcServer = grpc.NewServer(grpc.MaxMsgSize(1024 * 1024 * 20)) // 20mb
	service.RegisterMapReduceServer(w.grpcServer, w)
	go func() {
		w.l.Info("starting grpc server on address: %s", addr)
		if err := w.grpcServer.Serve(lis); err != nil {
			w.l.Error("error while running grpc server. err: %v", err)
		}
	}()

	return nil
}

// registerService registers worker service in consul.
func (w *Wrk) registerService() error {
	service := &api.AgentServiceRegistration{
		ID:      w.id,
		Name:    w.name,
		Port:    w.httpPort,
		Address: fmt.Sprintf("%s:%d", w.host, w.httpPort),
		Tags:    []string{GrpcConsulTag, fmt.Sprintf("%s:%d", w.host, w.rpcPort)},
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://%s:%d/health", w.host, w.httpPort),
			Interval: "1s",
			Timeout:  "1s",
		},
	}

	if err := w.consulClient.Agent().ServiceRegister(service); err != nil {
		return err
	}

	w.l.Info("registered service %s in consul!", w.name)
	return nil
}

// startHealthCheckServer starts the health check server for consul service discovery purpose.
func (w *Wrk) startHealthCheckServer() {
	// register endpoints and start http server
	go func() {
		// register consul health check endpoint
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "OK")
		})

		addr := fmt.Sprintf("%s:%d", w.host, w.httpPort)
		w.l.Info("starting http health check server on address: %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			w.l.Error("error while running health check http server. err: %v", err)
		}
	}()
}

// loadPlugin loads the gomr plugin with implementation of MapReduce interface.
func (w *Wrk) loadPlugin(job *service.MrJob) (MapReduce, error) {
	// check if plugin hasn't been already loaded
	sum := fmt.Sprintf("%x", md5.Sum(job.MapReducePlugin))
	w.l.Debug("mapReduce library checksum: %s", sum)
	val, ok := w.mrImplementations[sum]
	if ok {
		w.l.Debug("library already loaded - returning existing one.")
		return val, nil
	}

	w.l.Debug("loading new library %s", sum)
	// store library in work dir
	libPath := path.Join(w.workDir, job.WorkDir, fmt.Sprintf("%s-%d-%d.so", job.Name, time.Now().Unix(), time.Now().Nanosecond()))
	libFile, err := os.Create(libPath)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(libPath)

	writer := bufio.NewWriter(libFile)
	if _, err := writer.Write(job.MapReducePlugin); err != nil {
		return nil, err
	}

	if err := libFile.Close(); err != nil {
		return nil, err
	}

	lib, err := w.safeOpenPlugin(libPath)
	if err != nil {
		return nil, err
	}

	p, err := lib.Lookup("Impl")
	if err != nil {
		return nil, err
	}

	mrImpl, ok := p.(*MapReduce)
	if !ok {
		return nil, fmt.Errorf("wrong type of library. impl should be implementation of MapReduce interface. but is: %v", reflect.TypeOf(p))
	}

	w.mrImplementations[sum] = *mrImpl

	return *mrImpl, nil
}

// safeOpenPlugin encapsulates the opening of the plugin by calling the recover in the defer.
func (w *Wrk) safeOpenPlugin(path string) (plug *plugin.Plugin, err error) {
	defer func() {
		w.l.Debug("opening plugin '%s' finished", path)
		if r := recover(); r != nil {
			err = fmt.Errorf("pluginOpen panic: %v", r)
			w.l.Error("pluginOpen panic: %v. recovering", r)
		}
	}()

	w.l.Debug("opening plugin: %s", path)
	plug, err = plugin.Open(path)
	return
}
