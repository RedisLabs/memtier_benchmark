import tempfile
import threading
import redis
from collections import Counter

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


class ZipfianBenchmarkRunner:
    """Helper class to run Zipfian distribution benchmarks and collect key access data"""

    def __init__(
        self, env, key_min: int, key_max: int, threads: int = 2, clients: int = 10
    ):
        self.env = env
        self.key_min = key_min
        self.key_max = key_max
        self.threads = threads
        self.clients = clients

    def run_benchmark_and_collect_key_counting(
        self, test_name: str, zipf_exp: float = None
    ) -> Counter:
        """Run a complete benchmark and return key access distribution data"""
        # Create benchmark specs
        args = [
            "--ratio=1:1",  # Both SET and GET operations
            "--key-pattern=Z:Z",  # Zipfian for both SET and GET
            f"--key-minimum={self.key_min}",
            f"--key-maximum={self.key_max}",
        ]

        if zipf_exp is not None:
            args.append(f"--key-zipf-exp={zipf_exp}")

        benchmark_specs = {"name": test_name, "args": args}

        addTLSArgs(benchmark_specs, self.env)

        config = get_default_memtier_config(threads=self.threads, clients=self.clients)
        master_nodes_list = self.env.getMasterNodesList()
        overall_expected_request_count = get_expected_request_count(
            config, self.key_min, self.key_max
        )

        add_required_env_arguments(benchmark_specs, config, self.env, master_nodes_list)

        # Create temporary directory and run config
        test_dir = tempfile.mkdtemp()
        run_config = RunConfig(test_dir, test_name, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)

        benchmark = Benchmark.from_json(run_config, benchmark_specs)

        # Setup monitoring
        master_nodes_connections = self.env.getOSSMasterNodesConnectionList()

        monitor_threads = []
        for conn in master_nodes_connections:
            # prevent accumulating stats from previous runs
            conn.execute_command("CONFIG", "RESETSTAT")

            # start monitoring connection
            monitor_thread = MonitorThread(conn)
            monitor_thread.start()
            monitor_threads.append(monitor_thread)

        # Run the benchmark
        memtier_ok = benchmark.run()

        # Verify the benchmark ran successfully
        merged_command_stats = {
            "cmdstat_set": {"calls": 0},
            "cmdstat_get": {"calls": 0},
        }
        overall_request_count = agg_info_commandstats(
            master_nodes_connections, merged_command_stats
        )
        assert_minimum_memtier_outcomes(
            run_config,
            self.env,
            memtier_ok,
            overall_expected_request_count,
            overall_request_count,
        )

        """Collect and combine results from all monitor threads"""
        combined_key_counts = Counter()
        for thread in monitor_threads:
            thread.join()  # waits for monitor thread to finish
            combined_key_counts.update(thread.key_counts)

        return combined_key_counts
