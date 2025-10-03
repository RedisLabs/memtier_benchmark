/*
 * Copyright (C) 2011-2017 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with memtier_benchmark.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef MEMTIER_BENCHMARK_RUN_STATS_H
#define MEMTIER_BENCHMARK_RUN_STATS_H

#include <stdlib.h>
#include <stdio.h>
#include <map>
#include <vector>
#include <string>

#include "memtier_benchmark.h"
#include "run_stats_types.h"
#include "JSON_handler.h"
#include "deps/hdr_histogram/hdr_histogram.h"
#include "deps/hdr_histogram/hdr_histogram_log.h"


inline long long int ts_diff(struct timeval a, struct timeval b)
{
    unsigned long long aval = a.tv_sec * 1000000 + a.tv_usec;
    unsigned long long bval = b.tv_sec * 1000000 + b.tv_usec;

    return bval - aval;
}

enum tabel_el_type {
    string_el,
    double_el
};

struct table_el {
    tabel_el_type type;
    std::string format;
    std::string str_value;
    double  double_value;

    table_el* init_str(std::string fmt, std::string val) {
        type = string_el;
        format = fmt;
        str_value = val;
        return this;
    }

    table_el* init_double(std::string fmt, double val) {
        type = double_el;
        format = fmt;
        double_value = val;
        return this;
    }
};

struct table_column {
    table_column() {}
    table_column(unsigned int col_size) : column_size(col_size) {}

    unsigned int column_size;
    std::vector<table_el> elements;
};

class output_table {
private:
    std::vector<table_column> columns;

public:
    void print_header(FILE *out, const char * header);
    void add_column(table_column& col);
    void print(FILE *out, const char * header);
};

class run_stats {
protected:

    friend bool one_second_stats_predicate(const one_second_stats& a, const one_second_stats& b);

    benchmark_config *m_config;
    std::string m_cluster_topology_stdout;  // Stored cluster topology for stdout
    std::string m_cluster_topology_json;    // Stored cluster topology for JSON

    struct timeval m_start_time;
    struct timeval m_end_time;

    totals m_totals;

    std::list<one_second_stats> m_stats;
    std::vector<double> quantiles_list;

    // current second stats ( appended to m_stats and reset every second )
    one_second_stats m_cur_stats;

    safe_hdr_histogram m_get_latency_histogram;
    safe_hdr_histogram m_set_latency_histogram;
    safe_hdr_histogram m_wait_latency_histogram;
    std::vector<safe_hdr_histogram> m_ar_commands_latency_histograms;
    safe_hdr_histogram m_totals_latency_histogram;

    // instantaneous command stats ( used in the per second latencies )
    safe_hdr_histogram inst_m_get_latency_histogram;
    safe_hdr_histogram inst_m_set_latency_histogram;
    safe_hdr_histogram inst_m_wait_latency_histogram;
    std::vector<safe_hdr_histogram> inst_m_ar_commands_latency_histograms;
    safe_hdr_histogram inst_m_totals_latency_histogram;

    void roll_cur_stats(struct timeval* ts);

public:
    run_stats(benchmark_config *config);
    void setup_arbitrary_commands(size_t n_arbitrary_commands);
    void set_start_time(struct timeval* start_time);
    void set_end_time(struct timeval* end_time);

    void update_get_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency, unsigned int hits, unsigned int misses);
    void update_set_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_connection_error(struct timeval* ts);
    void update_active_connections(struct timeval* ts, unsigned int active_connections);

    void update_moved_get_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_moved_set_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_moved_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx,
                             unsigned int latency, size_t arbitrary_index);

    void update_ask_get_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_ask_set_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_ask_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx,
                                   unsigned int latency, size_t arbitrary_index);

    void update_wait_op(struct timeval* ts, unsigned int latency);
    void update_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx,
                             unsigned int latency, size_t arbitrary_index);

    void aggregate_average(const std::vector<run_stats>& all_stats);
    void summarize(totals& result) const;
    void summarize_current_second();
    void merge(const run_stats& other, int iteration);

    // Get current second's percentiles for real-time CSV export
    const std::vector<double>& get_current_set_percentiles() const { return m_cur_stats.m_set_cmd.summarized_quantile_values; }
    const std::vector<double>& get_current_get_percentiles() const { return m_cur_stats.m_get_cmd.summarized_quantile_values; }

    // Get stats list for aggregated percentile access
    const std::list<one_second_stats>& get_stats() const { return m_stats; }
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_get();
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_set();
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_wait();
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_totals();
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_arbitrary_command( unsigned int pos );
    std::vector<unsigned int> get_one_sec_cmd_stats_timestamp();
    void save_csv_one_sec(FILE *f,
                          unsigned long int& total_get_ops,
                          unsigned long int& total_set_ops,
                          unsigned long int& total_wait_ops);
    void save_csv_one_sec_cluster(FILE *f);
    void save_csv_set_get_commands(FILE *f, bool cluster_mode);
    void save_csv_arbitrary_commands_one_sec(FILE *f,
                                             arbitrary_command_list& command_list,
                                             std::vector<unsigned long int>& total_arbitrary_commands_ops);
    void save_csv_arbitrary_commands(FILE *f, arbitrary_command_list& command_list);
    bool save_hdr_percentiles_print_format(struct hdr_histogram* hdr, char* filename);
    bool save_hdr_log_format(struct hdr_histogram* hdr, char* filename, char* header);
    bool save_hdr_full_run(benchmark_config *config,int run_number);
    bool save_hdr_set_command(benchmark_config *config,int run_number);
    bool save_hdr_get_command(benchmark_config *config,int run_number);
    bool save_hdr_arbitrary_commands(benchmark_config *config,int run_number);

    bool save_csv(const char *filename, benchmark_config *config);
    static bool write_csv_header(FILE *f, benchmark_config *config);
    static bool write_csv_realtime_data(FILE *f, unsigned int second, unsigned int active_connections, unsigned int connection_errors,
                                       unsigned long int cur_ops, unsigned long int cur_bytes, double cur_latency, benchmark_config *config,
                                       const std::vector<double>* set_percentiles = nullptr, const std::vector<double>* get_percentiles = nullptr,
                                       const std::vector<double>* total_percentiles = nullptr, time_t start_time = 0);
    void debug_dump(void);

    // function to handle the results output
    bool print_arbitrary_commands_results();
    void print_type_column(output_table &table, arbitrary_command_list& command_list);
    void print_ops_sec_column(output_table &table);
    void print_hits_sec_column(output_table &table);
    void print_missess_sec_column(output_table &table);
    void print_moved_sec_column(output_table &table);
    void print_ask_sec_column(output_table &table);
    void print_avg_latency_column(output_table &table);
    void print_quantile_latency_column(output_table &table, double quantile, char* label);
    void print_kb_sec_column(output_table &table);
    void print_json(json_handler *jsonhandler, arbitrary_command_list& command_list, bool cluster_mode);
    void print_histogram(FILE *out, json_handler* jsonhandler, arbitrary_command_list& command_list);
    void print(FILE *file, benchmark_config *config,
               const char* header = NULL, json_handler* jsonhandler = NULL);

    // Cluster topology methods
    void capture_cluster_topology_data(class client_group *cg);
    void export_cluster_topology(FILE *file, json_handler* jsonhandler);

    unsigned int get_duration(void);
    unsigned long int get_duration_usec(void);
    unsigned long int get_total_bytes(void);
    unsigned long int get_total_ops(void);
    unsigned long int get_total_latency(void);
    unsigned long int get_total_connection_errors(void);
};

#endif //MEMTIER_BENCHMARK_RUN_STATS_H
