from collections import Counter
import math
from itertools import pairwise

from zipfian_benchmark_runner import ZipfianBenchmarkRunner


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


def analyze_zipfian_correlation(key_counts: Counter) -> float:
    """Analyze key distribution and return correlation coefficient for Zipf's law validation"""

    # Sort keys by frequency (descending)
    sorted_keys = sorted(key_counts.items(), key=lambda x: x[1], reverse=True)

    # Verify that frequency follows Zipf's law
    # in Zipfian, frequency of kth element is proportional to 1/k^s
    # so log(frequency) should be roughly linear with log(rank)
    ranks = list(range(1, len(sorted_keys) + 1))
    _, frequencies = zip(*sorted_keys)

    # Calculate correlation between log(rank) and log(frequency)
    log_ranks = [math.log(r) for r in ranks]
    log_freqs = [math.log(f) if f > 0 else 0 for f in frequencies]

    return correlation_coeficient(log_ranks, log_freqs)


def calculate_concentration_ratio(distribution: Counter, top_n: int = 10) -> float:
    """Calculate the concentration ratio of top N keys in the distribution"""
    top_n_sum = sum(sorted(distribution.values(), reverse=True)[:top_n])
    total_sum = sum(distribution.values())

    return top_n_sum / total_sum if total_sum > 0 else 0.0


def test_zipfian_key_distribution(env):
    """Test that the Zipfian key-pattern follows Zipf's law"""
    key_min = 1
    key_max = 10000

    # Use the helper class to run benchmark and collect data
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    combined_key_counts = runner.run_benchmark_and_collect_key_counting(env.testName)

    # Verify Zipfian properties using helper function
    correlation = analyze_zipfian_correlation(combined_key_counts)

    # should be close to -1 for a perfect law relationship
    # should be negative since log(rank) is increasing and log(freq) is decreasing
    env.assertTrue(correlation < -0.8)


def test_zipfian_exponent_effect(env):
    """Test different Zipfian exponents and verify they affect the distribution"""
    key_min = 1
    key_max = 10000

    # Test with different exponents
    zipf_exponents = {0.5, 1.0, 2.0}
    distributions = {}

    # Run benchmarks for each exponent
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)

    for zipf_exp in zipf_exponents:
        test_name = f"{env.testName}_exp_{zipf_exp}"
        combined_key_counts = runner.run_benchmark_and_collect_key_counting(
            test_name, zipf_exp=zipf_exp
        )
        distributions[zipf_exp] = combined_key_counts

    # Verify that higher exponents lead to more skewed distributions
    sorted_exponents = sorted(zipf_exponents)
    for exp1, exp2 in pairwise(sorted_exponents):
        dist1 = distributions[exp1]
        dist2 = distributions[exp2]

        concentration1 = calculate_concentration_ratio(dist1)
        concentration2 = calculate_concentration_ratio(dist2)

        # Higher exponent should have higher concentration in top keys
        env.assertTrue(concentration2 > concentration1)


def test_zipfian_arbitrary_command_set(env):
    """Test that arbitrary SET command with zipfian key pattern follows Zipf's law"""
    key_min = 1
    key_max = 10000

    # Use the helper class to run benchmark with arbitrary commands
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    commands = ["SET __key__ __data__"]
    combined_key_counts = runner.run_arbitrary_command_benchmark_and_collect_key_counting(
        env.testName, commands
    )

    # Verify Zipfian properties using helper function
    correlation = analyze_zipfian_correlation(combined_key_counts)

    # should be close to -1 for a perfect law relationship
    # should be negative since log(rank) is increasing and log(freq) is decreasing
    env.assertTrue(correlation < -0.8)


def test_zipfian_arbitrary_command_hset(env):
    """Test that arbitrary HSET command with zipfian key pattern follows Zipf's law"""
    key_min = 1
    key_max = 10000

    # Use the helper class to run benchmark with arbitrary commands
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    commands = ["HSET __key__ field1 __data__"]
    combined_key_counts = runner.run_arbitrary_command_benchmark_and_collect_key_counting(
        env.testName, commands
    )

    # Verify Zipfian properties using helper function
    correlation = analyze_zipfian_correlation(combined_key_counts)

    # should be close to -1 for a perfect law relationship
    env.assertTrue(correlation < -0.8)


def test_zipfian_arbitrary_command_mixed_operations(env):
    """Test multiple arbitrary commands with zipfian key pattern"""
    key_min = 1
    key_max = 10000

    # Use the helper class to run benchmark with multiple arbitrary commands
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    commands = [
        "SET __key__ __data__",
        "GET __key__",
        "HSET __key__ field1 __data__"
    ]
    combined_key_counts = runner.run_arbitrary_command_benchmark_and_collect_key_counting(
        env.testName, commands
    )

    # Verify Zipfian properties using helper function
    correlation = analyze_zipfian_correlation(combined_key_counts)

    # should be close to -1 for a perfect law relationship
    env.assertTrue(correlation < -0.8)

    # Verify that we have a reasonable distribution
    env.assertTrue(len(combined_key_counts) > 100)  # Should access many different keys
    env.assertTrue(sum(combined_key_counts.values()) > 1000)  # Should have many operations


def test_zipfian_arbitrary_command_exponent_effect(env):
    """Test different Zipfian exponents with arbitrary commands"""
    key_min = 1
    key_max = 10000

    # Test with different exponents
    zipf_exponents = [0.5, 1.0, 2.0]
    distributions = {}

    # Run benchmarks for each exponent
    runner = ZipfianBenchmarkRunner(env, key_min, key_max)
    commands = ["SET __key__ __data__"]

    for zipf_exp in zipf_exponents:
        test_name = f"{env.testName}_exp_{zipf_exp}"
        combined_key_counts = runner.run_arbitrary_command_benchmark_and_collect_key_counting(
            test_name, commands, zipf_exp=zipf_exp
        )
        distributions[zipf_exp] = combined_key_counts

    # Verify that higher exponents lead to more skewed distributions
    sorted_exponents = sorted(zipf_exponents)
    for i in range(len(sorted_exponents) - 1):
        exp1 = sorted_exponents[i]
        exp2 = sorted_exponents[i + 1]

        dist1 = distributions[exp1]
        dist2 = distributions[exp2]

        concentration1 = calculate_concentration_ratio(dist1)
        concentration2 = calculate_concentration_ratio(dist2)

        # Higher exponent should have higher concentration in top keys
        env.assertTrue(concentration2 > concentration1)
