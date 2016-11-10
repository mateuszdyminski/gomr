#!/usr/bin/env bash

usage() {
	cat <<EOF
Usage: $(basename $0) <command>

Wrappers around core binaries:
    consul-run             Runs the consul on the localhost.
    consul-run-bg          Runs the consul on the localhost in the background.
    consul-install         Installs consul in data dir.
    master-run             Runs the master service.
    worker-run             Runs the worker service.
    wordcount-run          Runs Word Count example.
EOF
	exit 1
}

CMD="$1"
shift
case "$CMD" in
	consul-run)
		consul agent -config-dir consul
	;;
	consul-run-bg)
		nohup consul agent -config-dir consul &
	;;
	consul-install)
		set -e
		echo 'Downloading consul 0.7.0 for linux amd64'
		wget https://releases.hashicorp.com/consul/0.7.0/consul_0.7.0_linux_amd64.zip
		unzip -o consul_0.7.0_linux_amd64.zip -d /tmp
		mv /tmp/consul consul/consul
		rm consul_0.7.0_linux_amd64.zip
		set +e
	;;
	master-run)
		go run master.go -host=localhost -http-port=8200 -debug=false
	;;
	worker-run)
		ID="$@"
		go run wrk.go -id=$ID -host=localhost -rpc-port=810$ID -http-port=820$ID -debug=true
	;;
	wordcount-run)
		cd examples/word_count; go run main.go --mrImplDir implementation wordCount.go --consulAddress localhost:8500
	;;
	*)
		usage
	;;
esac
