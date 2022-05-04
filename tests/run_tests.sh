#!/bin/bash

[[ $VERBOSE == 1 ]] && set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)

#----------------------------------------------------------------------------------------------

help() {
	cat <<-END
		Run flow tests.

		[ARGVARS...] run_tests.sh [--help|help]

		Argument variables:

		OSS_STANDALONE=0|1  General tests on standalone Redis (default)
		OSS_CLUSTER=0|1     General tests on Redis OSS Cluster
		TLS=0|1             Run tests with TLS enabled 
		SHARDS=n            Number of shards (default: 3)

		REDIS_SERVER=path   Location of redis-server
		VERBOSE=1           Print commands

	END
}

#----------------------------------------------------------------------------------------------

run_tests() {
	local title="$1"
	if [[ -n $title ]]; then
		printf "Running $title:\n\n"
	fi

	if [[ $VERBOSE == 1 ]]; then
		echo "RLTest configuration:"
		echo "$RLTEST_ARGS"
	fi

	cd $ROOT/tests

	local E=0
	{
		$OP python3 -m RLTest $RLTEST_ARGS
		((E |= $?))
	} || true

	return $E
}

#----------------------------------------------------------------------------------------------

[[ $1 == --help || $1 == help ]] && {
	help
	exit 0
}

#----------------------------------------------------------------------------------------------

OSS_STANDALONE=${OSS_STANDALONE:-1}
OSS_CLUSTER=${OSS_CLUSTER:-0}
SHARDS=${SHARDS:-3}
TEST=${TEST:-""}

TLS_KEY=$ROOT/tests/tls/redis.key
TLS_CERT=$ROOT/tests/tls/redis.crt
TLS_CACERT=$ROOT/tests/tls/ca.crt
REDIS_SERVER=${REDIS_SERVER:-redis-server}
MEMTIER_BINARY=$ROOT/memtier_benchmark

RLTEST_ARGS=" --oss-redis-path $REDIS_SERVER"
[[ "$TEST" != "" ]] && RLTEST_ARGS+=" --test $TEST"
[[ $VERBOSE == 1 ]] && RLTEST_ARGS+=" -v"
[[ $TLS == 1 ]] && RLTEST_ARGS+=" --tls-cert-file $TLS_CERT --tls-key-file $TLS_KEY --tls-ca-cert-file $TLS_CACERT --tls"

cd $ROOT/tests

E=0
[[ $OSS_STANDALONE == 1 ]] && {
	(TLS_KEY=$TLS_KEY TLS_CERT=$TLS_CERT TLS_CACERT=$TLS_CACERT MEMTIER_BINARY=$MEMTIER_BINARY RLTEST_ARGS="${RLTEST_ARGS}" run_tests "tests on OSS standalone")
	((E |= $?))
} || true

[[ $OSS_CLUSTER == 1 ]] && {
	(TLS_KEY=$TLS_KEY TLS_CERT=$TLS_CERT TLS_CACERT=$TLS_CACERT MEMTIER_BINARY=$MEMTIER_BINARY RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS" run_tests "tests on OSS cluster")
	((E |= $?))
} || true

exit $E
