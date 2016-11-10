### Requirements

* Protobuf github.com/google/protobuf
* Golang protoc wrapper (go get github.com/golang/protobuf/protoc-gen-go)

```
protoc ./mr.proto --go_out=plugins=grpc:.
```
