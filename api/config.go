package api

// Job describes the map reduce job.
type Job struct {
	Name           string
	MapTasksNum    int32
	ReduceTasksNum int32
	Input          string
	MrImplDir      string
	GoBinPath      string
}
