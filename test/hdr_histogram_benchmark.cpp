#include <benchmark/benchmark.h>
#include <hdr_histogram.h>
#include <math.h>
#include <random>

#ifdef _WIN32
#pragma comment ( lib, "Shlwapi.lib" )
#ifdef _DEBUG
#pragma comment ( lib, "benchmarkd.lib" )
#else
#pragma comment ( lib, "benchmark.lib" )
#endif
#endif

int64_t min_value = 1;
int64_t min_precision = 1;
int64_t max_precision = 4;
int64_t min_time_unit = 1000;
int64_t max_time_unit = 1000000;
int64_t step_time_unit = 1000;

static void generate_arguments_pairs(benchmark::internal::Benchmark *b)
{
    for (int64_t precision = min_precision; precision <= max_precision; precision++)
    {
        for (int64_t time_unit = min_time_unit; time_unit <= max_time_unit; time_unit *= step_time_unit)
        {
            b = b->ArgPair(precision, INT64_C(24) * 60 * 60 * time_unit);
        }
    }
}

static void BM_hdr_init(benchmark::State &state)
{
    const int64_t precision = state.range(0);
    const int64_t max_value = state.range(1);
    for (auto _ : state)
    {
        struct hdr_histogram *histogram;
        benchmark::DoNotOptimize(hdr_init(min_value, max_value, precision, &histogram));
        // read/write barrier
        benchmark::ClobberMemory();
    }
}

static void BM_hdr_record_values(benchmark::State &state)
{
    const int64_t precision = state.range(0);
    const int64_t max_value = state.range(1);
    struct hdr_histogram *histogram;
    hdr_init(min_value, max_value, precision, &histogram);
    benchmark::DoNotOptimize(histogram->counts);

    for (auto _ : state)
    {
        benchmark::DoNotOptimize(hdr_record_values(histogram, 1000000, 1));
        // read/write barrier
        benchmark::ClobberMemory();
    }
}

// Register the functions as a benchmark
BENCHMARK(BM_hdr_init)->Apply(generate_arguments_pairs);
BENCHMARK(BM_hdr_record_values)->Apply(generate_arguments_pairs);

BENCHMARK_MAIN();