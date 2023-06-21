import tempfile
import json
from include import *
from mbdirector.benchmark import Benchmark
from mbdirector.runner import RunConfig


def test_preload_and_set_get(env):
    key_max = 500000
    key_min = 1
    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10','--ratio=1:0','--key-pattern=P:P','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests='allkeys')
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)
    json_filename = '{0}/mb.json'.format(config.results_dir)

    for master_connection in master_nodes_connections:
        master_connection.execute_command("CONFIG", "RESETSTAT")

    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10','--ratio=1:1','--key-pattern=R:R','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests=200000)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)


def test_default_set(env):
    key_max = 500000
    key_min = 1
    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10','--ratio=1:0','--key-pattern=P:P','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests='allkeys')
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    master_nodes_connections = env.getOSSMasterNodesConnectionList()

    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)

    # ensure if we run again on a different key pattern the dataset doesn't grow
    for master_connection in master_nodes_connections:
        master_connection.execute_command("CONFIG", "RESETSTAT")

    benchmark_specs = {"name": env.testName, "args": ['--pipeline=10','--ratio=1:0','--key-pattern=R:R','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests=200000)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)

def test_default_set_get(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_default_set_get_with_print_percentiles(env):
    p_str = '0,10,20,30,40,50,60,70,80,90,95,100'
    histogram_prefix = 'percentiles-test'
    benchmark_specs = {"name": env.testName, "args": ['--print-percentiles={}'.format(p_str),'--hdr-file-prefix={}'.format(histogram_prefix)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)
    json_filename = '{0}/mb.json'.format(config.results_dir)

    hdr_files_sufix = ["_FULL_RUN_1","_SET_command_run_1","_GET_command_run_1"]
    histogram_files = []
    for sufix in hdr_files_sufix:
        for ftype in ["txt","hgrm"]:
            histogram_files.append("{0}{1}.{2}".format(histogram_prefix,sufix,ftype))

    ## Assert that all requested percentiles are stored at the json file
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        set_percentiles = results_dict['ALL STATS']['Sets']['Percentile Latencies']
        cleaned_keys = [ x.split(".")[0] for x in  set_percentiles.keys() ]
        for p in p_str.split(","):
            env.assertTrue("p{}".format(p) in cleaned_keys )
    
    # Assert that histogram output files are present
    for fname in histogram_files:
        env.assertTrue(os.path.isfile('{0}'.format(fname)))

    

# run each test on different env
def test_default_set_get_1_1(env):
    benchmark_specs = {"name": env.testName, "args": ['--ratio=1:1']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)

    # assert same number of gets and sets
    env.assertEqual(merged_command_stats['cmdstat_set']['calls'], merged_command_stats['cmdstat_get']['calls'])

# run each test on different env
def test_default_set_get_3_runs(env):
    run_count = 3
    benchmark_specs = {"name": env.testName, "args": ['--run-count={}'.format(run_count)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config) * run_count

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_default_arbitrary_command_pubsub(env):
    benchmark_specs = {"name": env.testName, "args": ['--command=publish \"__key__\" \"__data__\"']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    if not benchmark.run():
        debugPrintMemtierOnError(config, env)


def test_default_arbitrary_command_keyless(env):
    benchmark_specs = {"name": env.testName, "args": ['--command=PING']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    if not benchmark.run():
        debugPrintMemtierOnError(config, env)


def test_default_arbitrary_command_set(env):
    benchmark_specs = {"name": env.testName, "args": ['--command=SET __key__ __data__']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_default_arbitrary_command_hset(env):
    benchmark_specs = {"name": env.testName, "args": ['--command=HSET __key__ field1 __data__']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_hset': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_default_arbitrary_command_hset_multi_data_placeholders(env):
    benchmark_specs = {"name": env.testName, "args": ['--command=HSET __key__ field1 __data__ field2 __data__ field3 __data__']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_hset': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)
