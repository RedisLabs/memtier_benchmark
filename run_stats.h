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

#include "run_stats_types.h"
#include "JSON_handler.h"


typedef std::map<float, int> latency_map;
typedef std::map<float, int>::iterator latency_map_itr;
typedef std::map<float, int>::const_iterator latency_map_itr_const;

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

    struct timeval m_start_time;
    struct timeval m_end_time;

    totals m_totals;

    std::vector<one_second_stats> m_stats;
    one_second_stats m_cur_stats;

    latency_map m_get_latency_map;
    latency_map m_set_latency_map;
    latency_map m_wait_latency_map;
    void roll_cur_stats(struct timeval* ts);

public:
    run_stats();
    void set_start_time(struct timeval* start_time);
    void set_end_time(struct timeval* end_time);

    void update_get_op(struct timeval* ts, unsigned int bytes, unsigned int latency, unsigned int hits, unsigned int misses);
    void update_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency);

    void update_moved_get_op(struct timeval* ts, unsigned int bytes, unsigned int latency);
    void update_moved_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency);

    void update_ask_get_op(struct timeval* ts, unsigned int bytes, unsigned int latency);
    void update_ask_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency);

    void update_wait_op(struct timeval* ts, unsigned int latency);

    void aggregate_average(const std::vector<run_stats>& all_stats);
    void summarize(totals& result) const;
    void merge(const run_stats& other, int iteration);
    void save_csv_one_sec(FILE *f,
                          unsigned long int& total_get_ops,
                          unsigned long int& total_set_ops,
                          unsigned long int& total_wait_ops);
    void save_csv_one_sec_cluster(FILE *f);
    bool save_csv(const char *filename, bool cluster_mode);
    void debug_dump(void);
    void print(FILE *file, bool histogram, const char* header = NULL, json_handler* jsonhandler = NULL, bool cluster_mode = false);

    unsigned int get_duration(void);
    unsigned long int get_duration_usec(void);
    unsigned long int get_total_bytes(void);
    unsigned long int get_total_ops(void);
    unsigned long int get_total_latency(void);
};

#endif //MEMTIER_BENCHMARK_RUN_STATS_H
