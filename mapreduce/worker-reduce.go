package mapreduce

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/mateuszdyminski/gomr/service"
	"path"
	"strconv"
)

// Reduce starts reduce phase and returns the status of the tasks as long as its ongoing.
func (w *Wrk) Reduce(job *service.MrJob, server service.MapReduce_ReduceServer) error {
	reduceF, err := w.loadReduceFunc(job)
	if err != nil {
		return err
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_REDUCE, Status: service.Status_PLUGIN_LOADED}); err != nil {
		return err
	}

	currentReduceTask, err := strconv.Atoi(job.Input)
	if err != nil {
		return err
	}

	resultFile := mergeFileName(job.Name, currentReduceTask)

	outputF, err := os.Create(path.Join(w.workDir, job.WorkDir, resultFile))
	if err != nil {
		return err
	}

	// read all intermediate files into one map
	kvs := make(map[string][]string)
	intermediateFileName := reduceFileName(job.Name, int(job.MapTasksNum), currentReduceTask)

	input, err := os.Open(path.Join(w.workDir, job.WorkDir, intermediateFileName))
	if err != nil {
		return err
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_REDUCE, Status: service.Status_INPUT_LOADED}); err != nil {
		return err
	}

	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()

		vals := strings.Split(line, Separator)
		if len(vals) < 1 {
			w.l.Debug("wrong line format. Line %s", line)
			continue
		}
		key := vals[0]
		value := ""

		if len(vals) >= 2 {
			value = vals[1]
		}

		if _, ok := kvs[key]; ok {
			kvs[key] = append(kvs[key], value)
		} else {
			kvs[key] = []string{value}
		}
	}

	err = input.Close()
	if err != nil {
		return err
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_REDUCE, Status: service.Status_INTERMEDIATE_FILES_CREATED}); err != nil {
		return err
	}

	// sort values
	for key := range kvs {
		sort.Strings(kvs[key])
	}

	// write output
	for key, values := range kvs {

		result, err := w.safeReduceCall(reduceF, key, values)
		if err != nil {
			return err
		}

		if _, err := outputF.WriteString(fmt.Sprintf("%s %s\n", key, result)); err != nil {
			return err
		}
	}

	if err := outputF.Close(); err != nil {
		return err
	}

	if err := server.Send(&service.MrStatus{ServiceId: w.id, Phase: service.Phase_REDUCE, Status: service.Status_DONE}); err != nil {
		return err
	}

	return nil
}

// loadReduceFunc loads the plugin and returns the reduce function.
func (w *Wrk) loadReduceFunc(job *service.MrJob) (func(string, []string) string, error) {
	mrImple, err := w.loadPlugin(job)
	if err != nil {
		return nil, err
	}

	return mrImple.Reduce, nil
}

// safeReduceCall encapsulates the reduce function call by calling recover in the defer.
func (w *Wrk) safeReduceCall(reduceF func(string, []string) string, key string, vals []string) (result string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("reduceCall panic: %v", r)
			w.l.Error("reduceCall panic: %v. recovering", r)
		}
	}()

	result = reduceF(key, vals)
	return
}
