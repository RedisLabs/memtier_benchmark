# StatsD/UDP Real-Time Metrics Implementation for memtier_benchmark

## Project Goal

Add real-time metrics streaming to memtier_benchmark to visualize latency and QPS during Redis Enterprise scaling tests (specifically for Atomic Slot Migration / ASM testing). The implementation uses StatsD over UDP to send metrics to Graphite/Grafana for live visualization.

## Current Branch

```
git branch: feature/statsd-metrics
```

## Task List Status

| Status | Task | Description |
|--------|------|-------------|
| ⏳ IN_PROGRESS | Implement StatsD/UDP real-time metrics export | Parent task for the feature |
| ⏳ IN_PROGRESS | Create StatsD client implementation | Create statsd.h and statsd.cpp |
| ⬜ NOT_STARTED | Add command-line options for StatsD | Add --statsd-host, --statsd-port, --statsd-prefix |
| ⬜ NOT_STARTED | Integrate StatsD with metrics pipeline | Hook into main loop and roll_cur_stats() |
| ⬜ NOT_STARTED | Update build system | Add new source files to Makefile.am |
| ⬜ NOT_STARTED | Create Docker Compose setup | Graphite/StatsD + Grafana containers |
| ⬜ NOT_STARTED | Create Grafana dashboard | JSON dashboard for memtier metrics |
| ⬜ NOT_STARTED | Build and test integration | Compile and verify with Docker setup |

## Codebase Analysis Summary

### Current Metrics Flow

1. **Real-time stderr output** (memtier_benchmark.cpp:1720-1797):
   - Main loop polls threads every ~200ms
   - Outputs: ops/sec, bytes/sec, latency (current + average)
   - Key variables: `cur_ops_sec`, `cur_latency`, `avg_latency`, `ops_sec`

2. **Per-second stats collection** (run_stats.cpp):
   - `roll_cur_stats()` - rolls stats to new second bucket
   - `one_second_stats` - per-second aggregated data
   - `inst_m_*_latency_histogram` - instantaneous histograms (reset each second)

3. **Post-run outputs**:
   - JSON file (`--json-out-file`): Full time-series with `Time-Serie` section
   - CSV client stats (`--client-stats`): Per-client per-second data
   - HDR histograms (`--hdr-file-prefix`): Latency distributions

### Key Files to Modify

| File | Purpose |
|------|---------|
| `memtier_benchmark.h` | Add StatsD config fields to `benchmark_config` struct |
| `memtier_benchmark.cpp` | Add CLI options, initialize StatsD, emit metrics in main loop |
| `run_stats.cpp` | Optionally emit per-second stats in `roll_cur_stats()` |
| `Makefile.am` | Add new source files |

### Key Data Structures

```cpp
// memtier_benchmark.h - benchmark_config struct (line 52-129)
struct benchmark_config {
    // ... existing fields ...
    // ADD:
    const char *statsd_host;
    unsigned short statsd_port;
    const char *statsd_prefix;
};

// run_stats_types.h - one_sec_cmd_stats (line 76-105)
class one_sec_cmd_stats {
    unsigned long int m_bytes_rx, m_bytes_tx, m_ops;
    unsigned long long int m_total_latency;
    double m_avg_latency, m_min_latency, m_max_latency;
    // ... quantiles ...
};
```

### Main Loop Integration Point (memtier_benchmark.cpp:1789-1796)

```cpp
// After computing cur_ops_sec, cur_latency, etc:
if (total_connection_errors > 0) {
    fprintf(stderr, "[RUN #%u %.0f%%, %3u secs] ...\r", ...);
} else {
    fprintf(stderr, "[RUN #%u %.0f%%, %3u secs] ...\r", ...);
}
// INSERT StatsD emission here
```

## Implementation Plan

### 1. StatsD Client (statsd.h / statsd.cpp)

Simple UDP client with these functions:
- `statsd_init(host, port, prefix)` - create UDP socket
- `statsd_gauge(name, value)` - send gauge metric
- `statsd_timing(name, value_ms)` - send timing metric
- `statsd_counter(name, value)` - send counter metric
- `statsd_close()` - cleanup

Protocol format: `prefix.metric.name:value|type` (e.g., `memtier.ops_sec:15000|g`)

### 2. Metrics to Export

| Metric Name | Type | Description |
|-------------|------|-------------|
| `ops_sec` | gauge | Current ops/sec |
| `ops_sec_avg` | gauge | Average ops/sec |
| `bytes_sec` | gauge | Current throughput |
| `latency_ms` | timing | Current avg latency |
| `latency_avg_ms` | timing | Running avg latency |
| `latency_p50` | gauge | 50th percentile (per-second) |
| `latency_p99` | gauge | 99th percentile (per-second) |
| `latency_p999` | gauge | 99.9th percentile (per-second) |
| `connections` | gauge | Active connections |
| `connection_errors` | counter | Connection error count |

### 3. Command-Line Options

```
--statsd-host=HOST       StatsD server hostname (default: none, disabled)
--statsd-port=PORT       StatsD server port (default: 8125)
--statsd-prefix=PREFIX   Metric name prefix (default: memtier)
```

### 4. Docker Compose Setup

Create `docker-compose.statsd.yml` with:
- **graphite-statsd** container (graphiteapp/graphite-statsd) - receives StatsD UDP on 8125
- **grafana** container - visualization on port 3000
- Pre-configured Graphite datasource
- Pre-loaded dashboard

### 5. Grafana Dashboard

Dashboard panels:
- Ops/sec over time (current + average)
- Latency over time (avg, p50, p99, p99.9)
- Throughput (bytes/sec)
- Connection errors
- Progress indicator

## Files to Create

| File | Description |
|------|-------------|
| `statsd.h` | StatsD client header |
| `statsd.cpp` | StatsD client implementation |
| `docker-compose.statsd.yml` | Docker Compose for Graphite + Grafana |
| `grafana/provisioning/datasources/graphite.yml` | Auto-configure Graphite datasource |
| `grafana/provisioning/dashboards/dashboard.yml` | Dashboard provisioning config |
| `grafana/dashboards/memtier.json` | Grafana dashboard JSON |

## Code Snippets for Implementation

### statsd.h (to create)

```cpp
#ifndef _STATSD_H
#define _STATSD_H

class statsd_client {
private:
    int m_socket;
    struct sockaddr_in m_server;
    char m_prefix[256];
    bool m_enabled;

public:
    statsd_client();
    ~statsd_client();

    bool init(const char* host, unsigned short port, const char* prefix);
    void close();
    bool is_enabled() const { return m_enabled; }

    void gauge(const char* name, double value);
    void timing(const char* name, double value_ms);
    void counter(const char* name, long value);
};

#endif // _STATSD_H
```

### Integration in memtier_benchmark.cpp main loop

```cpp
// After line 1796, add:
if (statsd != NULL && statsd->is_enabled()) {
    statsd->gauge("ops_sec", cur_ops_sec);
    statsd->gauge("ops_sec_avg", ops_sec);
    statsd->gauge("bytes_sec", cur_bytes_sec);
    statsd->timing("latency_ms", cur_latency);
    statsd->timing("latency_avg_ms", avg_latency);
    statsd->gauge("connections", cfg->clients * active_threads);
    if (total_connection_errors > 0) {
        statsd->counter("connection_errors", total_connection_errors);
    }
}
```

## Testing Plan

1. Build memtier_benchmark with new code
2. Start Docker Compose: `docker-compose -f docker-compose.statsd.yml up -d`
3. Run memtier with StatsD: `./memtier_benchmark -s localhost --statsd-host=localhost --statsd-port=8125 --test-time=60`
4. Open Grafana at http://localhost:3000 (admin/admin)
5. View memtier dashboard with live metrics

## Helpful Commands

```bash
# Check current branch
git branch

# Build after changes
autoreconf -ivf
./configure
make clean && make

# Start Docker stack
docker-compose -f docker-compose.statsd.yml up -d

# View Grafana
open http://localhost:3000

# Test StatsD manually (send test metric)
echo "test.metric:100|g" | nc -u -w1 localhost 8125

# Stop Docker stack
docker-compose -f docker-compose.statsd.yml down
```

## References

- Main benchmark loop: `memtier_benchmark.cpp:1700-1832`
- Config struct: `memtier_benchmark.h:52-129`
- Per-second stats: `run_stats.cpp:174-182` (roll_cur_stats)
- CLI parsing: `memtier_benchmark.cpp:658-1230`
- Makefile sources: `Makefile.am`

## Next Immediate Steps

1. **Create `statsd.h`** - Header file with class definition
2. **Create `statsd.cpp`** - Implementation with UDP socket code
3. **Modify `memtier_benchmark.h`** - Add statsd config fields
4. **Modify `memtier_benchmark.cpp`** - Add CLI options and integration
5. **Update `Makefile.am`** - Add new source files
6. **Create Docker Compose files** - For local testing
7. **Create Grafana dashboard** - JSON dashboard definition
8. **Build and test** - Verify end-to-end functionality

