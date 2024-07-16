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

    benchmark_specs = {"name": env.testName, "args": ['--client-stats',f'{test_dir}/set_client_stats','--pipeline=10','--ratio=1:0','--key-pattern=R:R','--key-minimum={}'.format(key_min),'--key-maximum={}'.format(key_max)]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10, requests=200000)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,key_min, key_max)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    results_dir = config.results_dir
    ensure_clean_benchmark_folder(results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    merged_command_stats = {'cmdstat_set': {'calls': 0}}
    assert_keyspace_range(env, key_max, key_min, master_nodes_connections)

    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count,
                                    overall_request_count)

    # Assert that all CSV BW metrics are properly stored and calculated
    first_client_csv_stats = '{0}/set_client_stats-1-0-0.csv'.format(test_dir)
    found, set_tx_column_data = get_column_csv(first_client_csv_stats,"SET Total Bytes TX")
    env.assertTrue(found)
    found, set_rx_column_data = get_column_csv(first_client_csv_stats,"SET Total Bytes RX")
    env.assertTrue(found)
    found, set_tx_rx_column_data = get_column_csv(first_client_csv_stats,"SET Total Bytes")
    env.assertTrue(found)
    found, set_reqs_column_data = get_column_csv(first_client_csv_stats,"SET Requests")
    env.assertTrue(found)
    for col_pos, ops_sec in enumerate(set_reqs_column_data):
        if int(ops_sec) > 0:
            set_tx = int(set_tx_column_data[col_pos])
            set_rx = int(set_rx_column_data[col_pos])
            set_tx_rx = int(set_tx_rx_column_data[col_pos])
            env.assertTrue(set_tx > 0)
            env.assertTrue(set_rx > 0)
            env.assertTrue(set_tx_rx > 0)
            env.assertAlmostEqual(set_tx_rx,set_tx+set_rx,1)

    # the GET bw should be 0
    found, get_tx_column_data = get_column_csv(first_client_csv_stats,"GET Total Bytes TX")
    env.assertTrue(found)
    found, get_rx_column_data = get_column_csv(first_client_csv_stats,"GET Total Bytes RX")
    env.assertTrue(found)
    found, get_tx_rx_column_data = get_column_csv(first_client_csv_stats,"GET Total Bytes")
    env.assertTrue(found)
    for col_pos, ops_sec in enumerate(set_reqs_column_data):
        if int(ops_sec) > 0:
            get_tx = int(get_tx_column_data[col_pos])
            get_rx = int(get_rx_column_data[col_pos])
            get_tx_rx = int(get_tx_rx_column_data[col_pos])
            env.assertTrue(get_tx == 0)
            env.assertTrue(get_rx == 0)
            env.assertTrue(get_tx_rx == 0)
            env.assertAlmostEqual(set_tx_rx,set_tx+set_rx,1)

    ## Assert that all JSON BW metrics are properly stored and calculated
    json_filename = '{0}/mb.json'.format(config.results_dir)
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        set_metrics = results_dict['ALL STATS']['Sets']
        get_metrics = results_dict['ALL STATS']['Gets']
        set_metrics_ts = results_dict['ALL STATS']['Sets']["Time-Serie"]
        get_metrics_ts = results_dict['ALL STATS']['Gets']["Time-Serie"]
        for metric_name in ["KB/sec RX/TX","KB/sec RX","KB/sec TX","KB/sec"]:
            # assert the metric exists
            env.assertTrue(metric_name in set_metrics)
            env.assertTrue(metric_name in get_metrics)
            # assert the metric value is non zero on writes and zero on reads
            set_metric_value_kbs = set_metrics[metric_name]
            get_metric_value_kbs = get_metrics[metric_name]
            env.assertTrue(set_metric_value_kbs > 0)
            env.assertTrue(get_metric_value_kbs == 0)

        for second_data in set_metrics_ts.values():
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            count = second_data["Count"]
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                env.assertTrue(bytes_rx > 0)
                env.assertTrue(bytes_tx > 0)

        for second_data in get_metrics_ts.values():
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            # This test is write only so there should be no reads RX/TX and count
            count = second_data["Count"]
            env.assertTrue(count == 0)
            env.assertTrue(bytes_rx == 0)
            env.assertTrue(bytes_tx == 0)

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

    json_filename = '{0}/mb.json'.format(config.results_dir)
    ## Assert that all BW metrics are properly stored and calculated
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        set_metrics = results_dict['ALL STATS']['Sets']
        get_metrics = results_dict['ALL STATS']['Gets']
        set_metrics_ts = results_dict['ALL STATS']['Sets']["Time-Serie"]
        get_metrics_ts = results_dict['ALL STATS']['Gets']["Time-Serie"]
        for metric_name in ["KB/sec RX/TX","KB/sec RX","KB/sec TX","KB/sec"]:
            # assert the metric exists
            env.assertTrue(metric_name in set_metrics)
            env.assertTrue(metric_name in get_metrics)
            # assert the metric value is non zero given we've had write and read
            set_metric_value_kbs = set_metrics[metric_name]
            get_metric_value_kbs = get_metrics[metric_name]
            env.assertTrue(set_metric_value_kbs > 0)
            env.assertTrue(get_metric_value_kbs > 0)

        for second_data in set_metrics_ts.values():
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            count = second_data["Count"]
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                p50 = second_data["p50.00"]
                p99 = second_data["p99.00"]
                p999 = second_data["p99.90"]
                env.assertTrue(bytes_rx > 0)
                env.assertTrue(bytes_tx > 0)
                env.assertTrue(p50 > 0.0)
                env.assertTrue(p99 > 0.0)
                env.assertTrue(p999 > 0.0)

        for second_data in get_metrics_ts.values():
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            count = second_data["Count"]
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                p50 = second_data["p50.00"]
                p99 = second_data["p99.00"]
                p999 = second_data["p99.90"]
                env.assertTrue(bytes_rx > 0)
                env.assertTrue(bytes_tx > 0)
                env.assertTrue(p50 > 0.0)
                env.assertTrue(p99 > 0.0)
                env.assertTrue(p999 > 0.0)

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
def test_short_reconnect_interval(env):
    # cluster mode dose not support reconnect-interval option
    env.skipOnCluster()
    benchmark_specs = {"name": env.testName, "args": ['--reconnect-interval=1']}
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
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=publish \"__key__\" \"__data__\"')
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
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=PING')
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

    json_filename = '{0}/mb.json'.format(config.results_dir)
    ## Assert that all BW metrics are properly stored and calculated
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        metrics = results_dict['ALL STATS']['Pings']
        metrics_ts = results_dict['ALL STATS']['Pings']["Time-Serie"]
        totals_metrics_ts = results_dict['ALL STATS']['Totals']["Time-Serie"]
        for metric_name in ["KB/sec RX/TX","KB/sec RX","KB/sec TX","KB/sec"]:
            # assert the metric exists
            env.assertTrue(metric_name in metrics)
            # assert the metric value is non zero given we've had write and read
            metric_value_kbs = metrics[metric_name]
            env.assertTrue(metric_value_kbs > 0)

        totals_metrics_ts_v = list(totals_metrics_ts.values())
        for pos, second_data in enumerate(metrics_ts.values()):
            bytes_rx = second_data["Bytes RX"]
            bytes_tx = second_data["Bytes TX"]
            count = second_data["Count"]
            second_data_total = totals_metrics_ts_v[pos]
            for metric_name in ["p50.00","p99.00","p99.90"]:
                if count > 0:
                    metric_value_second_data = second_data[metric_name]
                    metric_value_totals_second_data = second_data_total[metric_name]
                    env.assertTrue(metric_value_totals_second_data == metric_value_second_data)
                    env.assertTrue(metric_value_second_data > 0.0)
            # if we had commands on that second the BW needs to be > 0
            if count > 0:
                env.assertTrue(bytes_rx > 0)
                env.assertTrue(bytes_tx > 0)


def test_default_arbitrary_command_set(env):
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=SET __key__ __data__')
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
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=HSET __key__ field1 __data__')
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
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    # on arbitrary command args should be the last one
    benchmark_specs["args"].append('--command=HSET __key__ field1 __data__ field2 __data__ field3 __data__')
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

def test_default_set_get_rate_limited(env):
    env.skipOnCluster()
    master_nodes_list = env.getMasterNodesList()
    for client_count in [1,2,4]:
        for thread_count in [1,2]:
            rps_per_client = 100
            test_time_secs = 5
            overall_expected_rps = rps_per_client * client_count * thread_count * len(master_nodes_list)
            overall_expected_request_count = test_time_secs * overall_expected_rps
            # we give a 1 sec margin
            request_delta = overall_expected_rps
            # we will specify rate limit and the test time, which should help us get an approximate request count
            benchmark_specs = {"name": env.testName, "args": ['--rate-limiting={}'.format(rps_per_client)]}
            addTLSArgs(benchmark_specs, env)
            config = get_default_memtier_config(thread_count,client_count,None,test_time_secs)

            master_nodes_connections = env.getOSSMasterNodesConnectionList()

            # reset the commandstats
            for master_connection in master_nodes_connections:
                master_connection.execute_command("CONFIG", "RESETSTAT")

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
            assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count, request_delta)


def test_data_import(env):
    env.skipOnCluster()
    benchmark_specs = {"name": env.testName, "args": [f"--data-import={ROOT_FOLDER}/tests/data-import-2-keys.txt",'--ratio=1:1']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,1, 2)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    # reset the commandstats
    for master_connection in master_nodes_connections:
        master_connection.execute_command("CONFIG", "RESETSTAT")

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    assert_keyspace_range(env, 2, 1, master_nodes_connections)

    merged_command_stats = {'cmdstat_set': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)


def test_data_import_setex(env):
    env.skipOnCluster()
    benchmark_specs = {"name": env.testName, "args": [f"--data-import={ROOT_FOLDER}/tests/data-import-2-keys-expiration.txt",'--ratio=1:1']}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config()
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config,1, 2)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    # reset the commandstats
    for master_connection in master_nodes_connections:
        master_connection.execute_command("CONFIG", "RESETSTAT")

    # benchmark.run() returns True if the return code of memtier_benchmark was 0
    memtier_ok = benchmark.run()

    assert_keyspace_range(env, 2, 1, master_nodes_connections)

    merged_command_stats = {'cmdstat_setex': {'calls': 0}, 'cmdstat_get': {'calls': 0}}
    overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
    assert_minimum_memtier_outcomes(config, env, memtier_ok, overall_expected_request_count, overall_request_count)
