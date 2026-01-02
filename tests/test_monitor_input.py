import tempfile
import os
from include import *
from mb import Benchmark, RunConfig


def test_monitor_input_specific_command(env):
    """
    Test that memtier_benchmark can use specific commands from a monitor input file.

    This test:
    1. Creates a monitor input file with multiple commands
    2. Uses __monitor_c1__ to select the first command (SET)
    3. Verifies the command executes correctly
    """
    # Create monitor input file
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write(
            '[ proxy49 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "key1" "value1"\n'
        )
        f.write('[ proxy47 ] 1764031576.603223 [0 172.16.10.147:39564] "GET" "key1"\n')
        f.write(
            '[ proxy48 ] 1764031576.605123 [0 172.16.10.147:41234] "HSET" "myhash" "field1" "value1"\n'
        )
        f.write(
            '[ proxy50 ] 1764031576.606456 [0 172.16.10.147:42567] "LPUSH" "mylist" "item1"\n'
        )
        f.write(
            '[ proxy51 ] 1764031576.607789 [0 172.16.10.147:43890] "SADD" "myset" "member1"\n'
        )

    # Configure memtier to use the first command from monitor file
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_c1__",  # Use first command (SET)
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=100)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
    env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))

    # Check that stderr shows the monitor file was loaded
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertTrue("Loaded 5 monitor commands from 5 total lines" in stderr_content)


    # Verify the key was created in Redis (standalone and OSS cluster-safe)
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    found = False
    for master_connection in master_nodes_connections:
        try:
            result = master_connection.execute_command("GET", "key1")
        except Exception:
            # In cluster mode, non-owner shards may reply MOVED/ASK; ignore and continue
            continue
        if result == b"value1":
            found = True
            break
    env.assertTrue(found)


def test_monitor_input_random_runtime(env):
    """
    Test that __monitor_c@__ picks random commands at runtime.

    This test:
    1. Creates a monitor input file with multiple different command types
    2. Uses __monitor_c@__ to randomly select commands at runtime
    3. Verifies that multiple different command types were executed
    """
    # Create monitor input file with diverse commands
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write(
            '[ proxy49 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "key1" "value1"\n'
        )
        f.write('[ proxy47 ] 1764031576.603223 [0 172.16.10.147:39564] "GET" "key1"\n')
        f.write(
            '[ proxy48 ] 1764031576.605123 [0 172.16.10.147:41234] "HSET" "myhash" "field1" "value1"\n'
        )
        f.write(
            '[ proxy50 ] 1764031576.606456 [0 172.16.10.147:42567] "LPUSH" "mylist" "item1"\n'
        )
        f.write(
            '[ proxy51 ] 1764031576.607789 [0 172.16.10.147:43890] "SADD" "myset" "member1"\n'
        )

    # Configure memtier to use random commands from monitor file
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
	            "--command=__monitor_c@__",  # Command selection at runtime
	            "--monitor-pattern=R",  # Random selection
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=2, clients=2, requests=100)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
    env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))

    # Check that stderr shows the monitor file was loaded
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertTrue("Loaded 5 monitor commands from 5 total lines" in stderr_content)

    # Verify that multiple different data types were created in Redis
    # This proves that different commands were executed
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    types_found = set()

    for master_connection in master_nodes_connections:
        # Check for different key types
        keys_to_check = [
            ("key1", "string"),
            ("myhash", "hash"),
            ("mylist", "list"),
            ("myset", "set"),
        ]

        for key, expected_type in keys_to_check:
            try:
                key_type = master_connection.execute_command("TYPE", key)
                if isinstance(key_type, bytes):
                    key_type = key_type.decode("utf-8")
                if key_type == expected_type:
                    types_found.add(expected_type)
            except:
                pass

    # We should have at least 2 different types, proving randomization worked
    env.debugPrint("Types found: {}".format(types_found), True)
    env.assertTrue(len(types_found) >= 2)
    if len(types_found) < 2:
        env.debugPrint(
            "Expected at least 2 different data types, found: {}".format(types_found),
            True,
        )


def test_monitor_input_sequential_default(env):
	    """
	    Test that __monitor_c@__ picks commands sequentially when monitor-pattern is explicitly set to S.

	    This test:
	    1. Creates a monitor input file with multiple SET commands for the same key but different values
	    2. Uses __monitor_c@__ with --monitor-pattern=S (sequential pattern, which is also the default)
	    3. Verifies that the commands are applied in sequential order (with wrap-around)
	    """
	    # Create monitor input file with sequential SET commands
	    test_dir = tempfile.mkdtemp()
	    monitor_file = os.path.join(test_dir, "monitor.txt")
	    with open(monitor_file, "w") as f:
	        f.write(
	            '[ proxy60 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "seq_key" "v1"\n'
	        )
	        f.write(
	            '[ proxy61 ] 1764031576.605123 [0 172.16.10.147:41234] "SET" "seq_key" "v2"\n'
	        )
	        f.write(
	            '[ proxy62 ] 1764031576.606456 [0 172.16.10.147:42567] "SET" "seq_key" "v3"\n'
	        )

	    # Configure memtier to use sequential commands from monitor file with explicit pattern S
	    benchmark_specs = {
	        "name": env.testName,
	        "args": [
	            "--monitor-input={}".format(monitor_file),
	            "--command=__monitor_c@__",  # Sequential selection
	            "--monitor-pattern=S",       # Explicit sequential pattern
	        ],
	    }
	    addTLSArgs(benchmark_specs, env)

	    # 4 requests: expect sequence q1, q2, q3, q1
	    config = get_default_memtier_config(threads=1, clients=1, requests=4)
	    master_nodes_list = env.getMasterNodesList()

	    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

	    config = RunConfig(test_dir, env.testName, config, {})
	    ensure_clean_benchmark_folder(config.results_dir)

	    benchmark = Benchmark.from_json(config, benchmark_specs)

	    # Run memtier_benchmark
	    memtier_ok = benchmark.run()

	    # Verify success
	    debugPrintMemtierOnError(config, env)
	    env.assertTrue(memtier_ok == True)
	    env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
	    env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))

	    # Verify the final value for seq_key corresponds to the expected sequence with wrap-around (v1)
	    # This must work both on standalone and OSS cluster deployments.
	    master_nodes_connections = env.getOSSMasterNodesConnectionList()
	    found = False
	    for master_connection in master_nodes_connections:
	        try:
	            result = master_connection.execute_command("GET", "seq_key")
	        except Exception:
	            # In cluster mode, non-owner shards may reply MOVED/ASK; ignore and continue
	            continue
	        if result == b"v1":
	            found = True
	            break
	    env.assertTrue(found)


def test_monitor_input_mixed_commands(env):
    """
    Test mixing specific and random monitor commands with command ratios.

    This test:
    1. Creates a monitor input file
    2. Uses 30% __monitor_c1__ (specific SET command) and 70% __monitor_c@__ (random)
    3. Verifies both command types execute correctly
    """
    # Create monitor input file
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write(
            '[ proxy49 ] 1764031576.604009 [0 172.16.10.147:51682] "SET" "key1" "value1"\n'
        )
        f.write('[ proxy47 ] 1764031576.603223 [0 172.16.10.147:39564] "GET" "key1"\n')
        f.write(
            '[ proxy48 ] 1764031576.605123 [0 172.16.10.147:41234] "HSET" "myhash" "field1" "value1"\n'
        )
        f.write(
            '[ proxy50 ] 1764031576.606456 [0 172.16.10.147:42567] "LPUSH" "mylist" "item1"\n'
        )
        f.write(
            '[ proxy51 ] 1764031576.607789 [0 172.16.10.147:43890] "SADD" "myset" "member1"\n'
        )

    # Configure memtier with mixed commands
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--monitor-input={}".format(monitor_file),
            "--command=__monitor_c1__",
            "--command-ratio=30",
            "--command=__monitor_c@__",
            "--command-ratio=70",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=1, requests=100)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Run memtier_benchmark
    memtier_ok = benchmark.run()

    # Verify success
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile("{0}/mb.stdout".format(config.results_dir)))
    env.assertTrue(os.path.isfile("{0}/mb.stderr".format(config.results_dir)))

    # Check that stderr shows the monitor file was loaded
    with open("{0}/mb.stderr".format(config.results_dir)) as stderr:
        stderr_content = stderr.read()
        env.assertTrue("Loaded 5 monitor commands from 5 total lines" in stderr_content)


    # Verify key1 exists (from the specific SET command) in a cluster-safe way
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    found = False
    for master_connection in master_nodes_connections:
        try:
            result = master_connection.execute_command("GET", "key1")
        except Exception:
            # In cluster mode, non-owner shards may reply MOVED/ASK; ignore and continue
            continue
        if result == b"value1":
            found = True
            break
    env.assertTrue(found)
