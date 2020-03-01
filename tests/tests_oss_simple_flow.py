import glob
import os

from mbdirector.benchmark import Benchmark
from mbdirector.runner import RunConfig

MEMTIER_BINARY = os.environ.get("MEMTIER_BINARY","memtier_benchmark")

# run each test on different env
def test_default_set_get(env):
    benchmark_specs = {"name": env.testName, "args": []}
    config = {
        "memtier_benchmark": {
            "binary": MEMTIER_BINARY,
            "threads": 10,
            "clients": 5,
            "requests": 1000
        },
    }
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(env.testName, "", config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)


# run each test on different env
def test_default_set_get_1_1(env):
    benchmark_specs = {"name": env.testName, "args": ['--ratio=1:1']}
    config = {
        "memtier_benchmark": {
            "binary": MEMTIER_BINARY,
            "threads": 10,
            "clients": 5,
            "requests": 1000
        },
    }
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(env.testName, "", config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()


    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count, overall_request_count)

    # assert same number of gets and sets
    env.assertEqual(merged_command_stats['cmdstat_set']['calls'], merged_command_stats['cmdstat_get']['calls'])



# run each test on different env
def test_default_set_get_3_runs(env):
    run_count=3
    benchmark_specs = {"name": env.testName, "args": ['--run-count={}'.format(run_count)]}
    config = {
        "memtier_benchmark": {
            "binary": MEMTIER_BINARY,
            "threads": 10,
            "clients": 5,
            "requests": 1000
        },
    }
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)*run_count

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(env.testName, "", config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()


    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count, overall_request_count)


def assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count, overall_request_count):
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


def get_expected_request_count(config):
    result = -1
    if 'memtier_benchmark' in config:
        mt = config['memtier_benchmark']
        if 'threads' in mt and 'clients' in mt and 'requests' in mt:
            result= config['memtier_benchmark']['threads'] * config['memtier_benchmark']['clients'] * \
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


def ensure_clean_benchmark_folder(dirname):
    files = glob.glob('{}/*'.format(dirname))
    for f in files:
        os.remove(f)
    if os.path.exists(dirname):
        os.removedirs(dirname)
    os.makedirs(dirname)
