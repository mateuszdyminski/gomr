package mapreduce

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/mateuszdyminski/gomr/service"
	"strings"
)

// Map starts map phase and returns the status of the tasks as long as its ongoing.
func (w *Wrk) Map(job *service.MrJob, server service.MapReduce_MapServer) error {
	mapF, err := w.loadMapFunc(job)
	if err != nil {
		return err
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_MAP, Status: service.Status_PLUGIN_LOADED}); err != nil {
		return err
	}

	f, err := os.Open(job.Input)
	if err != nil {
		return err
	}
	defer f.Close()

	all, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_MAP, Status: service.Status_INPUT_LOADED}); err != nil {
		return err
	}

	kvs, err := w.safeMapCall(mapF, job.Input, all)
	if err != nil {
		return err
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_MAP, Status: service.Status_DONE}); err != nil {
		return err
	}

	// TODO: improve disc writes - use buffered writer
	for _, kv := range kvs {
		intermediateFileName := reduceFileName(job.Name, int(job.MapTasksNum), int(fnvHash(kv.Key))%int(job.ReduceTasksNum))
		outputF, err := os.OpenFile(path.Join(w.workDir, job.WorkDir, intermediateFileName), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			return err
		}

		if _, err = outputF.WriteString(fmt.Sprintf("%s%s%s\n", kv.Key, Separator, kv.Value)); err != nil {
			return err
		}

		if err = outputF.Close(); err != nil {
			return err
		}
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_MAP, Status: service.Status_INTERMEDIATE_FILES_CREATED}); err != nil {
		return err
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_MAP, Status: service.Status_DONE}); err != nil {
		return err
	}

	return nil
}

// loadMapFunc loads the plugin and returns the map function.
func (w *Wrk) loadMapFunc(job *service.MrJob) (fun func(string, string) []KeyValue, err error) {
	mrImple, err := w.loadPlugin(job)
	if err != nil {
		return nil, err
	}

	return mrImple.Map, nil
}

// safeMapCall encapsulates the map function call by calling recover in the defer.
func (w *Wrk) safeMapCall(mapF func(string, string) []KeyValue, input string, content []byte) (result []KeyValue, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mapCall panic: %v", r)
			w.l.Error("mapCall panic: %v. recovering", r)
		}
	}()

	for _, line := range strings.Split(string(content), "\n") {
		vals := mapF(input, line)
		result = append(result, vals...)
	}
	return
}
