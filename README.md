## Gomr

Map-reduce framework based on https://tip.golang.org/pkg/plugin/. 
It allows to submit job with your own implementation of Map-Reduce. The compilation of Map-Reduce implementation is done before the job submission and then compiled plugin is transferred to the workers where its load and run against the input files described in the job. 

### Requirements

- Go in version 1.8 or above
- gRPC - go get google.golang.org/grpc

### Architecture

![Architecture](https://github.com/mateuszdyminski/gomr/raw/master/presentation/data/gomr.png)

### Components

#### Gomr - client

- Is responsible for submitting job to the gomr cluster
- Connects to the Consul to get the address of master service
- Tracks the progress of computation
- Job submit blocks the invocation

#### Gomr - master

- Connects to the Consul to get the address of all worker services
- Watches for new workers or removes dead ones
- Prepares files for the map phase
- Merges files after the reduce phase
- Reruns the failed tasks
- Gets the current status of work and pass it to the client

#### Gomr - worker

- Loads plugin implementation
- Sends status of work to master
- Computes map and reduce phase
- Prepares intermediate files for reduce phase - partitioning

### Gomr limitations

- No distributed filesystem
- Due to no DFS everything is run on the same machine
- Split phase loads everything into memory
- Partitioning phase loads everything into memory
- Merge phase loads everything into memory

### Example of Map-Reduce implementation

```go
package main

import (
	"fmt"
	"github.com/mateuszdyminski/gomr/mapreduce"
	"strconv"
	"strings"
	"unicode"
)

func main() {}

// MrImpl implements the gomr MapReduce interface.
type MrImpl struct{}

// Map analyzes the each line of the input file and returns the number of occurrences of word.
func (mr MrImpl) Map(key, value string) (result []mapreduce.KeyValue) {
	isNotLetter := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(value, isNotLetter)

	result = make([]mapreduce.KeyValue, 0, len(words))
	for _, w := range words {
		result = append(result, mapreduce.KeyValue{Key: strings.ToLower(w), Value: strconv.Itoa(1)})
	}
	return
}

// Reduce calculates the number of particular word(key).
func (mr MrImpl) Reduce(key string, values []string) string {
	counter := 0
	for _, v := range values {
		val, err := strconv.Atoi(v)
		if err != nil {
			continue
		}
		counter += val
	}

	return fmt.Sprintf("%d", counter)
}

// Impl exports the implementation of MapReduce to be available for plugin.Lookup.
var Impl mapreduce.MapReduce = MrImpl{}
```

### Example of job submission 

```go
package main

import (
	"flag"
	"log"

	"github.com/mateuszdyminski/gomr/api"
)

var (
	mrImplDir     = flag.String("mrImplDir", "implementation", "A directory with the implementation of MapReduce interface")
	consulAddress = flag.String("consulAddress", "localhost:8500", "Address of Consul")
)

func main() {
	flag.Parse()

	job := api.Job{
		Name:           "WordCount",
		Input:          "/Users/md/workspace/go/src/github.com/mateuszdyminski/gomr/data/chapter1",
		MapTasksNum:    5,
		ReduceTasksNum: 1,
		MrImplDir:      *mrImplDir,
		GoBinPath:      "/Users/md/workspace/gosrc/go/bin/go",
	}

	client, err := api.NewMrClient(*consulAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("submitting job %s!\n", job.Name)

	summary, err := client.Submit(job)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("status: %+v \n", summary.Status)
	log.Printf("result: %+v \n", summary.Result)
	log.Printf("duration: %+v \n", summary.Duration)
}

```

### How to run gomr cluster

Install Consul on Mac
```
./dev.sh consul-install-mac
```
or Linux
```
./dev.sh consul-install-linux
```

Run Consul
```
./dev.sh consul-run
```

Run Master - in new terminal
```
./dev.sh master-run
```

Run some workers - each in new terminal
```
./dev.sh worker-run ID
```
where ID is 1,2,3,4...N where N is less than 10

### How to submit word-count job

```
./dev.sh wordcount-run
```

Results should be in 'results/WordCount/<TIMESTAMP>/mrtmp.WordCount'

### Inspirations 

- [Go-Plugins](https://tip.golang.org/pkg/plugin/)
- [MIT distributed systems course](https://pdos.csail.mit.edu/6.824/)
- [Glow project](https://github.com/chrislusf/glow)
- [Glow Advent article](https://blog.gopheracademy.com/advent-2015/glow-map-reduce-for-golang/) especially last paragraph: 

'Glow has limitations that Go code can not be sent and executed remotely.'
