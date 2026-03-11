"""
Tests for client staircase ramp-up feature.

Validates that --clients-start, --clients-step, and --step-duration work
correctly: correct CLI validation, proper JSON output structure, and that
the per-second active client counts follow the expected staircase pattern.
"""
import tempfile
import json
import os

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    debugPrintMemtierOnError,
    MEMTIER_BINARY,
)
from mb import Benchmark, RunConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_staircase_benchmark(env, test_dir, clients, clients_start,
                                clients_step, step_duration, test_time,
                                threads=2, extra_args=None):
    """Build a benchmark configured for staircase ramp-up."""
    config = get_default_memtier_config(threads=threads, clients=clients,
                                        test_time=test_time)
    # Clear requests since we use test_time
    config['memtier_benchmark']['requests'] = None

    master_nodes_list = env.getMasterNodesList()

    args = [
        '--clients-start', str(clients_start),
        '--clients-step', str(clients_step),
        '--step-duration', str(step_duration),
        '--ratio', '1:1',
        '--key-prefix', 'staircase-test-',
    ]
    if extra_args:
        args.extend(extra_args)

    benchmark_specs = {"name": env.testName, "args": args}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    return benchmark, run_config


def _load_json(run_config):
    """Load and return the JSON results dict."""
    json_path = os.path.join(run_config.results_dir, "mb.json")
    with open(json_path) as f:
        return json.load(f)


def _expected_clients_at_second(ts, clients_start, clients_step,
                                 step_duration, clients_max):
    """Calculate expected clients per thread at a given second."""
    steps_done = ts // step_duration
    active = clients_start + steps_done * clients_step
    return min(active, clients_max)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_staircase_json_config_fields(env):
    """Verify JSON configuration section includes staircase parameters."""
    env.skipOnCluster()

    clients = 10
    clients_start = 2
    clients_step = 2
    step_duration = 2
    test_time = 8

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=2)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)

            # Check configuration section has staircase fields
            cfg = results.get("configuration", {})
            env.assertEqual(cfg.get("clients"), clients,
                            message="JSON config 'clients' should be max target")
            env.assertEqual(cfg.get("clients_start"), clients_start,
                            message="JSON config should have clients_start")
            env.assertEqual(cfg.get("clients_step"), clients_step,
                            message="JSON config should have clients_step")
            env.assertEqual(cfg.get("step_duration"), step_duration,
                            message="JSON config should have step_duration")
            env.assertEqual(cfg.get("test_time"), test_time,
                            message="JSON config should have test_time")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_active_clients_section_exists(env):
    """Verify JSON output contains 'Active Clients' section with entries."""
    env.skipOnCluster()

    clients = 10
    clients_start = 2
    clients_step = 2
    step_duration = 2
    test_time = 8

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=2)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})

            # Active Clients section must exist
            env.assertTrue("Active Clients" in all_stats,
                           message="JSON ALL STATS should contain 'Active Clients'")

            ac = all_stats["Active Clients"]
            env.assertGreater(len(ac), 0,
                              message="Active Clients should have per-second entries")

            # Each entry must have the required fields
            for ts_str, entry in ac.items():
                env.assertTrue("Clients per thread" in entry,
                               message="Each Active Clients entry should have 'Clients per thread'")
                env.assertTrue("Total clients" in entry,
                               message="Each Active Clients entry should have 'Total clients'")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_active_clients_pattern(env):
    """Verify per-second active client counts follow the staircase pattern.

    With clients=10, clients_start=2, clients_step=2, step_duration=3,
    test_time=15, threads=2:
      t=0-2: 2 clients/thread (4 total)
      t=3-5: 4 clients/thread (8 total)
      t=6-8: 6 clients/thread (12 total)
      t=9-11: 8 clients/thread (16 total)
      t=12-14: 10 clients/thread (20 total)
    """
    env.skipOnCluster()

    clients = 10
    clients_start = 2
    clients_step = 2
    step_duration = 3
    test_time = 15
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})
            ac = all_stats.get("Active Clients", {})

            # Verify each second's client count matches the expected staircase
            for ts_str, entry in ac.items():
                ts = int(ts_str)
                expected_per_thread = _expected_clients_at_second(
                    ts, clients_start, clients_step, step_duration, clients)
                expected_total = expected_per_thread * threads

                env.assertEqual(entry["Clients per thread"], expected_per_thread,
                                message="At t={}s, expected {} clients/thread, got {}".format(
                                    ts, expected_per_thread, entry["Clients per thread"]))
                env.assertEqual(entry["Total clients"], expected_total,
                                message="At t={}s, expected {} total clients, got {}".format(
                                    ts, expected_total, entry["Total clients"]))

            # Verify staircase is monotonically non-decreasing
            prev = 0
            for ts_str in sorted(ac.keys(), key=int):
                cur = ac[ts_str]["Clients per thread"]
                env.assertTrue(cur >= prev,
                               message="Staircase should be non-decreasing: t={}, prev={}, cur={}".format(
                                   ts_str, prev, cur))
                prev = cur

            # Verify we start at clients_start
            if "0" in ac:
                env.assertEqual(ac["0"]["Clients per thread"], clients_start,
                                message="First second should have clients_start clients")

        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_total_clients_equals_per_thread_times_threads(env):
    """Verify Total clients = Clients per thread * threads for all entries."""
    env.skipOnCluster()

    clients = 12
    clients_start = 3
    clients_step = 3
    step_duration = 2
    test_time = 10
    threads = 3

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            ac = results.get("ALL STATS", {}).get("Active Clients", {})

            for ts_str, entry in ac.items():
                per_thread = entry["Clients per thread"]
                total = entry["Total clients"]
                env.assertEqual(total, per_thread * threads,
                                message="At t={}s, Total clients ({}) should be "
                                        "Clients per thread ({}) * threads ({})".format(
                                            ts_str, total, per_thread, threads))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_max_clients_reached(env):
    """Verify clients reach the max value and stay there."""
    env.skipOnCluster()

    clients = 8
    clients_start = 2
    clients_step = 2
    step_duration = 2
    test_time = 12
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            ac = results.get("ALL STATS", {}).get("Active Clients", {})

            # Max should be reached at step_duration * num_steps = 2 * 3 = 6s
            max_reached = False
            for ts_str in sorted(ac.keys(), key=int):
                ts = int(ts_str)
                per_thread = ac[ts_str]["Clients per thread"]
                if per_thread == clients:
                    max_reached = True
                # Once max is reached, it should stay at max
                if max_reached:
                    env.assertEqual(per_thread, clients,
                                    message="After reaching max, clients should stay at {} "
                                            "(got {} at t={}s)".format(clients, per_thread, ts))

            env.assertTrue(max_reached,
                           message="Should reach max clients ({}) within test_time".format(clients))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_ops_produced_every_step(env):
    """Verify that ops are produced in every step of the staircase, proving
    that clients added at each step are actively sending requests."""
    env.skipOnCluster()

    clients = 10
    clients_start = 2
    clients_step = 4
    step_duration = 4
    test_time = 12
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})
            totals_ts = all_stats.get("Totals", {}).get("Time-Serie", {})
            totals_count = all_stats.get("Totals", {}).get("Count", 0)

            # Overall ops should be non-zero
            env.assertGreater(totals_count, 0,
                              message="Total ops should be greater than zero")

            # Each second in the time-series should have ops
            for ts_str, entry in totals_ts.items():
                count = entry.get("Count", 0)
                env.assertGreater(count, 0,
                                  message="Ops at t={}s should be > 0".format(ts_str))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_no_staircase_no_active_clients_section(env):
    """Verify that when staircase is NOT used, there is no Active Clients section."""
    env.skipOnCluster()

    config = get_default_memtier_config(threads=2, clients=5, test_time=3)
    config['memtier_benchmark']['requests'] = None

    master_nodes_list = env.getMasterNodesList()
    benchmark_specs = {
        "name": env.testName,
        "args": ['--ratio', '1:1', '--key-prefix', 'no-staircase-test-'],
    }
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    test_dir = tempfile.mkdtemp()
    try:
        run_config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)
        benchmark = Benchmark.from_json(run_config, benchmark_specs)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            all_stats = results.get("ALL STATS", {})

            env.assertFalse("Active Clients" in all_stats,
                            message="Non-staircase run should NOT have Active Clients section")

            # Also verify config section doesn't have staircase fields
            cfg = results.get("configuration", {})
            env.assertFalse("clients_start" in cfg,
                            message="Non-staircase config should not have clients_start")
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass


def test_staircase_clients_never_exceed_max(env):
    """Verify that clients per thread never exceed --clients max."""
    env.skipOnCluster()

    # Use a step size that doesn't divide evenly into max
    clients = 10
    clients_start = 3
    clients_step = 4   # 3 -> 7 -> 10 (capped, not 11)
    step_duration = 3
    test_time = 12
    threads = 2

    test_dir = tempfile.mkdtemp()
    try:
        benchmark, run_config = _build_staircase_benchmark(
            env, test_dir, clients=clients, clients_start=clients_start,
            clients_step=clients_step, step_duration=step_duration,
            test_time=test_time, threads=threads)

        ok = benchmark.run()
        failed_asserts = env.getNumberOfFailedAssertion()
        try:
            env.assertTrue(ok, message="memtier_benchmark should exit successfully")

            results = _load_json(run_config)
            ac = results.get("ALL STATS", {}).get("Active Clients", {})

            for ts_str, entry in ac.items():
                per_thread = entry["Clients per thread"]
                env.assertTrue(per_thread <= clients,
                               message="Clients per thread ({}) should never exceed max ({}) "
                                       "at t={}s".format(per_thread, clients, ts_str))
                env.assertTrue(per_thread >= clients_start,
                               message="Clients per thread ({}) should never be below start ({}) "
                                       "at t={}s".format(per_thread, clients_start, ts_str))

            # Verify the uneven step: should go 3 -> 7 -> 10 (not 11)
            expected_sequence = [3, 7, 10]
            seen_values = sorted(set(entry["Clients per thread"]
                                     for entry in ac.values()))
            env.assertEqual(seen_values, expected_sequence,
                            message="Expected staircase values {} but got {}".format(
                                expected_sequence, seen_values))
        finally:
            if env.getNumberOfFailedAssertion() > failed_asserts:
                debugPrintMemtierOnError(run_config, env)
    finally:
        pass
