import tempfile
import threading
from collections import Counter
import redis
import math

import redis.client

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    ensure_clean_benchmark_folder,
    addTLSArgs,
    agg_info_commandstats,
    assert_minimum_memtier_outcomes,
    get_expected_request_count,
)
from mbdirector.benchmark import Benchmark
from mbdirector.runner import RunConfig


class MonitorThread(threading.Thread):
    """Monitor Redis commands and count key accesses"""

    def __init__(self, connection, stop_commands: set[str] = None):
        threading.Thread.__init__(self)
        self.monitor: redis.client.Monitor = connection.monitor()
        self.stop_commands: set[str] = stop_commands or {
            "INFO COMMANDSTATS",
            "FLUSHALL",
        }

        self.key_counts: Counter = Counter()

    def run(self):
        try:
            with self.monitor as m:
                for command_info in m.listen():
                    command = command_info.get("command")

                    if command.upper() in self.stop_commands:
                        break

                    parts = command.split()
                    if len(parts) >= 2 and parts[0].upper() in {"SET", "GET"}:
                        key = parts[1]
                        self.key_counts[key] += 1

        except redis.ConnectionError:
            # stop monitoring: server connection was closed
            pass


def correlation_coeficient(x: list[float], y: list[float]) -> float:
    """Calculate Pearson correlation coefficient between two lists of numbers"""
    n = len(x)
    mean_x = sum(x) / n
    mean_y = sum(y) / n

    numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))

    sum_sq_x = sum((x[i] - mean_x) ** 2 for i in range(n))
    sum_sq_y = sum((y[i] - mean_y) ** 2 for i in range(n))
    denominator = math.sqrt(sum_sq_x * sum_sq_y)

    return numerator / denominator if denominator != 0 else 0


def test_zipfian_key_distribution(env):
    """Test that the Zipfian key-pattern follows Zipf's law"""
    key_min = 1
    key_max = 10000

    # Configure benchmark with Zipfian distribution
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--ratio=1:1",  # Both SET and GET operations
            "--key-pattern=Z:Z",  # Zipfian for both SET and GET
            f"--key-minimum={key_min}",
            f"--key-maximum={key_max}",
        ],
    }

    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=2, clients=10)
    master_nodes_list = env.getMasterNodesList()
    overall_expected_request_count = get_expected_request_count(
        config, key_min, key_max
    )

    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    # Create a temporary directory
    test_dir = tempfile.mkdtemp()
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)

    monitor_threads = []
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    # Start monitoring Redis commands
    for conn in master_nodes_connections:
        monitor_thread = MonitorThread(conn)
        monitor_thread.start()
        monitor_threads.append(monitor_thread)

    # Run the benchmark
    memtier_ok = benchmark.run()

    # Verify the benchmark ran successfully
    merged_command_stats = {"cmdstat_set": {"calls": 0}, "cmdstat_get": {"calls": 0}}
    overall_request_count = agg_info_commandstats(
        master_nodes_connections, merged_command_stats
    )
    assert_minimum_memtier_outcomes(
        config, env, memtier_ok, overall_expected_request_count, overall_request_count
    )

    # Combine results from all master nodes' monitor threads
    combined_key_counts = Counter()
    for thread in monitor_threads:
        thread.join()  # waits for monitor thread to finish
        combined_key_counts.update(thread.key_counts)

    # Verify Zipfian properties
    if len(combined_key_counts) > 0:
        # Sort keys by frequency (ascending)
        sorted_keys = sorted(
            combined_key_counts.items(), key=lambda x: x[1], reverse=True
        )

        # Verify that frequency follows Zipf's law
        # in Zipfian, frequency of kth element is proportional to 1/k^s
        # so log(frequency) should be roughly linear with log(rank)
        ranks = list(range(1, len(sorted_keys) + 1))
        _, frequencies = zip(*sorted_keys)

        # Calculate correlation between log(rank) and log(frequency)
        log_ranks = [math.log(r) for r in ranks]
        log_freqs = [math.log(f) if f > 0 else 0 for f in frequencies]
        correlation = correlation_coeficient(log_ranks, log_freqs)

        env.debugPrint(
            f"Correlation between log(rank) and log(frequency): {correlation}"
        )

        # should be close to -1 for a perfect law relationship
        # should be negative since log(rank) is increasing and log(freq) is decreasing
        env.assertTrue(correlation < -0.8)
