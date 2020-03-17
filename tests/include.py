import glob
import os

MEMTIER_BINARY = os.environ.get("MEMTIER_BINARY", "memtier_benchmark")
TLS_CERT = os.environ.get("TLS_CERT", "")
TLS_KEY = os.environ.get("TLS_KEY", "")
TLS_CACERT = os.environ.get("TLS_CACERT", "")


def assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count):
    # assert correct exit code
    env.assertTrue(memtier_ok == True)
    # assert we have all outputs
    env.assertTrue(os.path.isfile('{0}/mb.stdout'.format(config.results_dir)))
    env.assertTrue(os.path.isfile('{0}/mb.stderr'.format(config.results_dir)))
    env.assertTrue(os.path.isfile('{0}/mb.json'.format(config.results_dir)))

    # assert we have the expected request count
    env.assertEqual(overall_expected_request_count, overall_request_count)


def add_required_env_arguments(benchmark_specs, config, env, master_nodes_list):
    # check if environment is cluster
    if env.isCluster():
        benchmark_specs["args"].append("--cluster-mode")
    # check if environment uses Unix Socket connections
    if env.isUnixSocket():
        benchmark_specs["args"].append("--unix-socket")
        benchmark_specs["args"].append(master_nodes_list[0]['unix_socket_path'])
        config["memtier_benchmark"]['explicit_connect_args'] = True
    else:
        config['redis_process_port'] = master_nodes_list[0]['port']


def debugPrintMemtierOnError(config, env, memtier_ok):
    if not memtier_ok:
        with open('{0}/mb.stderr'.format(config.results_dir)) as stderr:
            env.debugPrint("### PRINTING STDERR OUTPUT OF MEMTIER ON FAILURE ###", True)
            env.debugPrint("### mb.stderr file location: {0}".format('{0}/mb.stderr'.format(config.results_dir)), True)
            for line in stderr:
                env.debugPrint(line.rstrip(), True)

        with open('{0}/mb.stdout'.format(config.results_dir)) as stderr:
            env.debugPrint("### PRINTING STDERR OUTPUT OF MEMTIER ON FAILURE ###", True)
            env.debugPrint("### mb.stderr file location: {0}".format('{0}/mb.stdout'.format(config.results_dir)), True)
            for line in stderr:
                env.debugPrint(line.rstrip(), True)

        if not env.isCluster():
            if env.envRunner is not None:
                log_file = os.path.join(env.envRunner.dbDirPath, env.envRunner._getFileName('master', '.log'))
                with open(log_file) as redislog:
                    env.debugPrint("### REDIS LOG ###", True)
                    env.debugPrint(
                        "### log_file file location: {0}".format(log_file), True)
                    for line in redislog:
                        env.debugPrint(line.rstrip(), True)


def get_expected_request_count(config):
    result = -1
    if 'memtier_benchmark' in config:
        mt = config['memtier_benchmark']
        if 'threads' in mt and 'clients' in mt and 'requests' in mt:
            result = config['memtier_benchmark']['threads'] * config['memtier_benchmark']['clients'] * \
                     config['memtier_benchmark']['requests']
    return result


def agg_info_commandstats(master_nodes_connections, merged_command_stats):
    overall_request_count = 0
    for master_connection in master_nodes_connections:
        shard_stats = master_connection.execute_command("INFO", "COMMANDSTATS")
        for cmd_name, cmd_stat in shard_stats.items():
            if cmd_name in merged_command_stats:
                overall_request_count += cmd_stat['calls']
                merged_command_stats[cmd_name]['calls'] = merged_command_stats[cmd_name]['calls'] + cmd_stat['calls']
    return overall_request_count


def addTLSArgs(benchmark_specs, env):
    if env.useTLS:
        benchmark_specs['args'].append('--tls')
        benchmark_specs['args'].append('--cert={}'.format(TLS_CERT))
        benchmark_specs['args'].append('--key={}'.format(TLS_KEY))
        benchmark_specs['args'].append('--cacert={}'.format(TLS_CACERT))


def get_default_memtier_config():
    config = {
        "memtier_benchmark": {
            "binary": MEMTIER_BINARY,
            "threads": 10,
            "clients": 5,
            "requests": 1000
        },
    }
    return config


def ensure_clean_benchmark_folder(dirname):
    files = glob.glob('{}/*'.format(dirname))
    for f in files:
        os.remove(f)
    if os.path.exists(dirname):
        os.removedirs(dirname)
    os.makedirs(dirname)
