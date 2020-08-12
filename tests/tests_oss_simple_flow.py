import tempfile
import json
from include import *
from mbdirector.benchmark import Benchmark
from mbdirector.runner import RunConfig


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
    debugPrintMemtierOnError(config, env, memtier_ok)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)


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
    debugPrintMemtierOnError(config, env, memtier_ok)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)
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
    debugPrintMemtierOnError(config, env, memtier_ok)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)

    # assert same number of gets and sets
    env.assertEqual(merged_command_stats['cmdstat_set']['calls'], merged_command_stats['cmdstat_get']['calls'])


# run each test on different env
def test_default_set_get_3_runs(env):
    env.skipOnCluster()
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
    debugPrintMemtierOnError(config, env, memtier_ok)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, merged_command_stats, overall_expected_request_count,
                                    overall_request_count)
