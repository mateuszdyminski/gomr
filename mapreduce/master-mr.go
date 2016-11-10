package mapreduce

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/mateuszdyminski/gomr/service"
)

// mrTask holds info about the single task - whether it's map or reduce task.
type mrTask struct {
	// input holds input param for the task
	input string

	// errs keeps the history of all errors
	errs []error

	// ok flag informs whether the task has been finished with success or not
	ok bool

	// retries describes how many times particular task has been retried due to errors while calculation
	retries int
}

// addErr adds the error at the end of slice of errors.
func (m *mrTask) addErr(err error) {
	m.errs = append(m.errs, err)
}

// lastErr returns the last occurred error if any - otherwise returns nil.
func (m *mrTask) lastErr() error {
	if len(m.errs) == 0 {
		return nil
	}

	return m.errs[len(m.errs)-1]
}

// runReducePhase runs whole reduce phase on the remote workers.
func (m *Master) runReducePhase(job service.MrJob, statuses chan *service.MrStatus) error {
	// prepare the reduce tasks which should be calculated on the workers
	tasks := make(chan mrTask, job.ReduceTasksNum)
	for iReduce := 0; iReduce < int(job.ReduceTasksNum); iReduce++ {
		tasks <- mrTask{
			input: strconv.Itoa(iReduce),
		}
	}

	return m.runPhase(ReducePhase, job, tasks, statuses)
}

// runMapPhase runs whole map phase on the remote workers.
func (m *Master) runMapPhase(job service.MrJob, files []string, statuses chan *service.MrStatus) error {
	// prepare the map tasks which should be calculated on the workers
	tasks := make(chan mrTask, len(files))
	for _, file := range files {
		tasks <- mrTask{
			input: file,
		}
	}

	return m.runPhase(MapPhase, job, tasks, statuses)
}

// runPhase runs phase{map, reduce} on the remote workers. It's responsible for worker coordination.
func (m *Master) runPhase(phase string, job service.MrJob, tasks chan mrTask, statuses chan *service.MrStatus) error {
	m.mutex.Lock()

	// run
	tasksNo := len(tasks)
	quitJob := make(chan bool)
	results := make(chan mrTask)
	for _, wrk := range m.workers {
		go m.runTask(wrk, phase, job, tasks, quitJob, statuses, results)
	}

	m.mutex.Unlock()

	// we should wait for all tasks to be finished
	acc := 0
	for finishedTask := range results {
		// check is some task exceeds the failures limit
		if !finishedTask.ok {
			return finishedTask.lastErr()
		}

		acc++

		// check if all tasks are already finished
		if acc >= tasksNo {
			quitJob <- true
			close(tasks)
			return nil
		}
	}

	return nil
}

// runTask runs single taks{map, reduce} on the remote worker.
func (m *Master) runTask(
	wrk *wrk,
	phase string,
	job service.MrJob,
	tasks chan mrTask,
	quitJob chan bool,
	statuses chan *service.MrStatus,
	results chan mrTask) {

	for {
		select {
		case task, ok := <-tasks:
			if !ok {
				return
			}

			// check if the number of retries exceeds the limit - if so - cancel the whole job
			if task.retries >= RetriesLimit {
				m.l.Error("[%s] [%s] task: %s exceeded the retries limit: %d. cancelling the job!", wrk.id, phase, task.input, task.retries)
				task.addErr(fmt.Errorf("number of errors for task: %s exeeds the limit: %d", task.input, RetriesLimit))
				results <- task
				return
			}

			job.Input = task.input
			m.l.Info("[%s] [%s] starting task for job(%s)! input: %s", wrk.id, phase, job.Name, job.Input)
			stream, err := m.runPhaseFunc(wrk, phase, job)
			if err != nil {
				m.l.Error("[%s] [%s] err while running job(%s). file: %s. try no.: %d. err: %v", wrk.id, phase, job.Name, job.Input, task.retries, err)
				// TODO: it's error connected with worker - not with job - consider removing the worker from the pool
				task.retries++
				task.addErr(err)
				tasks <- task
				continue
			}

			// drain stream channel
			for {
				status, err := stream.Recv()
				if err == io.EOF {
					m.l.Info("[%s] [%s] task for job(%s) done! file: %s", wrk.id, phase, job.Name, job.Input)
					task.ok = true
					results <- task
					break
				}

				if err != nil {
					m.l.Error("[%s] [%s] error while running job(%s). file: %s. try no.: %d. err: %v", wrk.id, phase, job.Name, job.Input, task.retries, err)
					stream.CloseSend()
					task.retries++
					task.addErr(err)
					tasks <- task
					break
				}

				statuses <- status
				m.l.Info("[%s] [%s] task status(%v)", wrk.id, phase, status.Status)
			}
		case <-quitJob:
			return
		}
	}
}

func (m *Master) runPhaseFunc(wrk *wrk, phase string, job service.MrJob) (service.MapReduce_MapClient, error) {
	switch phase {
	case MapPhase:
		return wrk.grpcClient.Map(context.Background(), &job)
	case ReducePhase:
		return wrk.grpcClient.Reduce(context.Background(), &job)
	default:
		return nil, fmt.Errorf("wrong phase: %s", phase)
	}
}
