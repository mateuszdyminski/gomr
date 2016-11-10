package mapreduce

// KeyValue tuple with key and value.
type KeyValue struct {
	Key   string
	Value string
}

// MapReduce it's interface for any job in gomr framework. Every gomr job should implement this interface.
type MapReduce interface {
	Map(string, string) []KeyValue
	Reduce(string, []string) string
}
