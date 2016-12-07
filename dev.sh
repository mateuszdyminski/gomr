#!/usr/bin/env bash

usage() {
	cat <<EOF
Usage: $(basename $0) <command>

Wrappers around core binaries:
    consul-run             Runs the consul on the localhost.
    consul-run-bg          Runs the consul on the localhost in the background.
    consul-install-linux   Installs consul in data dir.
    consul-install-mac     Installs consul in data dir.
    master-run             Runs the master service.
    worker-run             Runs the worker service.
    wordcount-run          Runs Word Count example.
    lotto-run              Runs Lotto example.
EOF
	exit 1
}

GO=${GOTIP:-$(which go)}

CMD="$1"
shift
case "$CMD" in
	consul-run)
		consul/consul agent -config-dir consul
	;;
	consul-run-bg)
		nohup consul/consul agent -config-dir consul &
	;;
	consul-install-linux)
		set -e
		echo 'Downloading consul 0.7.1 for linux amd64'
		wget https://releases.hashicorp.com/consul/0.7.1/consul_0.7.1_linux_amd64.zip
		unzip -o consul_0.7.1_linux_amd64.zip -d /tmp
		mv /tmp/consul consul/consul
		rm consul_0.7.1_linux_amd64.zip
		set +e
	;;
	consul-install-mac)
		set -e
		echo 'Downloading consul 0.7.1 for darwin amd64'
		wget https://releases.hashicorp.com/consul/0.7.1/consul_0.7.1_darwin_amd64.zip
		unzip -o consul_0.7.1_darwin_amd64.zip -d /tmp
		mv /tmp/consul consul/consul
		rm consul_0.7.1_darwin_amd64.zip
		set +e
	;;
	master-run)
		$GO run master.go -host=localhost -http-port=8200 -debug=false
	;;
	worker-run)
		ID="$@"
		$GO run wrk.go -id=$ID -host=localhost -rpc-port=810$ID -http-port=820$ID -debug=true
	;;
	wordcount-run)
		cd examples/word_count; $GO run main.go --mrImplDir implementation wordCount.go --consulAddress localhost:8500
	;;
	lotto-run)
		cd examples/lotto; $GO run main.go --mrImplDir implementation lotto.go --consulAddress localhost:8500
	;;
	*)
		usage
	;;
esac
