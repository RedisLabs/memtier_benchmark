import tempfile
import json
import subprocess
import time
from include import *
from mb import Benchmark, RunConfig


def test_cpu_stats_in_json(env):
    """Verify CPU stats appear in JSON output with valid structure."""
    benchmark_specs = {"name": env.testName, "args": []}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=5, requests=1000)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(config)

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    if not memtier_ok:
        debugPrintMemtierOnError(config, env)

    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile('{0}/mb.json'.format(config.results_dir)))

    json_filename = '{0}/mb.json'.format(config.results_dir)
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)

        # CPU Stats should be present under ALL STATS
        env.assertTrue('ALL STATS' in results_dict)
        all_stats = results_dict['ALL STATS']
        env.assertTrue('CPU Stats' in all_stats)

        cpu_stats = all_stats['CPU Stats']
        env.assertTrue(len(cpu_stats) > 0)

        for second_key, second_data in cpu_stats.items():
            # Each second should have Main Thread
            env.assertTrue('Main Thread' in second_data)
            main_cpu = second_data['Main Thread']
            env.assertTrue(main_cpu >= 0)
            env.assertTrue(main_cpu < 100)

            # Each second should have Thread N entries matching thread count (2)
            for t in range(2):
                thread_key = 'Thread {}'.format(t)
                env.assertTrue(thread_key in second_data)
                thread_cpu = second_data[thread_key]
                env.assertTrue(thread_cpu >= 0)
                env.assertTrue(thread_cpu < 100)


def test_cpu_stats_high_load(env):
    """Stress a single thread with many clients to drive high CPU and verify warning."""
    env.skipOnCluster()

    # 1 thread, 500 clients, deep pipeline, small data, time-based run
    # This should saturate the single worker thread and trigger the >95% CPU warning
    benchmark_specs = {"name": env.testName, "args": [
        '--pipeline=100',
        '--data-size=1',
        '--ratio=1:1',
        '--key-pattern=R:R',
        '--key-maximum=100',
    ]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=500, requests=None, test_time=5)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    if not memtier_ok:
        debugPrintMemtierOnError(config, env)

    env.assertTrue(memtier_ok == True)
    env.assertTrue(os.path.isfile('{0}/mb.json'.format(config.results_dir)))

    # Verify JSON CPU stats exist and show non-trivial CPU usage
    json_filename = '{0}/mb.json'.format(config.results_dir)
    with open(json_filename) as results_json:
        results_dict = json.load(results_json)
        all_stats = results_dict['ALL STATS']
        env.assertTrue('CPU Stats' in all_stats)

        cpu_stats = all_stats['CPU Stats']
        env.assertTrue(len(cpu_stats) >= 2)

        # With 500 clients on 1 thread, the worker should be using significant CPU
        max_thread_cpu = 0
        for second_key, second_data in cpu_stats.items():
            env.assertTrue('Thread 0' in second_data)
            thread_cpu = second_data['Thread 0']
            env.assertTrue(thread_cpu >= 0)
            env.assertTrue(thread_cpu < 100)
            if thread_cpu > max_thread_cpu:
                max_thread_cpu = thread_cpu

        # The single worker thread should be using meaningful CPU (> 10%)
        env.debugPrint("Max worker thread CPU observed: {:.1f}%".format(max_thread_cpu), True)
        env.assertTrue(max_thread_cpu > 10.0)

    # Check stderr for the high CPU warning
    stderr_filename = '{0}/mb.stderr'.format(config.results_dir)
    if os.path.isfile(stderr_filename):
        with open(stderr_filename) as stderr_file:
            stderr_content = stderr_file.read()
            has_warning = 'WARNING: High CPU on thread' in stderr_content
            env.debugPrint("High CPU warning present in stderr: {}".format(has_warning), True)
            if max_thread_cpu > 95.0:
                # If we observed >95% CPU, the warning must have fired
                env.assertTrue(has_warning)


def test_cpu_stats_external_validation(env):
    """Cross-validate memtier CPU stats against psutil external measurements."""
    try:
        import psutil
    except ImportError:
        env.debugPrint("psutil not available, skipping external CPU validation", True)
        return

    env.skipOnCluster()

    num_threads = 1
    test_time = 5

    benchmark_specs = {"name": env.testName, "args": [
        '--pipeline=100',
        '--data-size=1',
        '--ratio=1:1',
        '--key-pattern=R:R',
        '--key-maximum=100',
    ]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=num_threads, clients=500,
                                        requests=None, test_time=test_time)
    master_nodes_list = env.getMasterNodesList()

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    # Launch as subprocess (non-blocking) so we can sample CPU externally
    process = subprocess.Popen(
        benchmark.args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Sample external CPU via psutil
    external_samples = []
    access_denied = False
    try:
        p = psutil.Process(process.pid)
        prev_threads = {t.id: (t.user_time, t.system_time) for t in p.threads()}
        prev_time = time.time()
        time.sleep(1)

        while process.poll() is None:
            cur_time = time.time()
            wall_delta = cur_time - prev_time
            if wall_delta < 0.5:
                time.sleep(0.5)
                continue

            cur_threads = {t.id: (t.user_time, t.system_time) for t in p.threads()}
            sample = {}
            for tid, (cu, cs) in cur_threads.items():
                if tid in prev_threads:
                    pu, ps_time = prev_threads[tid]
                    delta_cpu = (cu - pu) + (cs - ps_time)
                    sample[tid] = (delta_cpu / wall_delta) * 100.0
            external_samples.append(sample)

            prev_threads = cur_threads
            prev_time = cur_time
            time.sleep(1)
    except psutil.NoSuchProcess:
        pass
    except psutil.AccessDenied:
        # macOS task_for_pid restriction — can't read child process threads
        access_denied = True

    stdout, stderr = process.communicate()
    if stderr:
        benchmark.write_file('mb.stderr', stderr)

    env.assertTrue(process.returncode == 0)

    if access_denied:
        env.debugPrint("psutil.AccessDenied (macOS task_for_pid restriction), skipping", True)
        return

    env.assertTrue(len(external_samples) >= 2)

    # Read memtier JSON output
    json_filename = os.path.join(config.results_dir, 'mb.json')
    env.assertTrue(os.path.isfile(json_filename))

    with open(json_filename) as f:
        results_dict = json.load(f)

    cpu_stats = results_dict['ALL STATS']['CPU Stats']

    # Compute average total process CPU% from memtier JSON (all threads)
    internal_total_cpus = []
    for sec_key, sec_data in cpu_stats.items():
        sec_total = sum(v for k, v in sec_data.items())
        internal_total_cpus.append(sec_total)
    internal_avg = sum(internal_total_cpus) / len(internal_total_cpus)

    # Compute average total process CPU% from psutil (all threads)
    external_total_cpus = []
    for sample in external_samples:
        sec_total = sum(sample.values())
        external_total_cpus.append(sec_total)
    external_avg = sum(external_total_cpus) / len(external_total_cpus)

    env.debugPrint("Internal avg total CPU: {:.1f}%".format(internal_avg), True)
    env.debugPrint("External avg total CPU: {:.1f}%".format(external_avg), True)
    env.debugPrint("Delta: {:.1f}pp".format(abs(internal_avg - external_avg)), True)

    # Assert they agree within 25 percentage points
    # psutil may see additional internal threads (libevent, I/O) not reported by memtier,
    # so the external total can be higher; use a wider tolerance to account for this.
    env.assertTrue(abs(internal_avg - external_avg) < 25.0)

    # Verify both show significant CPU usage (not both near zero)
    env.assertTrue(internal_avg > 10.0)
    env.assertTrue(external_avg > 10.0)
