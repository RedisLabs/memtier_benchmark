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

#include <string.h>
#include <sys/time.h>
#include <math.h>
#include <algorithm>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "run_stats.h"
#include "memtier_benchmark.h"

void output_table::add_column(table_column& col) {
    assert(columns.empty() || columns[0].elements.size() == col.elements.size());
    columns.push_back(col);
}

void output_table::print_header(FILE *out, const char * header) {
    if (header == NULL)
        return;

    fprintf(out, "\n\n");
    fprintf(out, "%s\n", header);

    for (unsigned int i=0; i<columns.size(); i++) {
        fprintf(out,"============");
    }

    fprintf(out,"\n");
}

void output_table::print(FILE *out, const char * header) {
    print_header(out, header);

    int num_of_elements = columns[0].elements.size();

    for (int i=0; i<num_of_elements; i++) {
        std::string line;
        char buf[100];

        for (unsigned int j=0; j<columns.size(); j++) {
            table_el* el = &columns[j].elements[i];
            switch (el->type) {
                case string_el:
                    snprintf(buf, 100, el->format.c_str(), el->str_value.c_str());
                    break;
                case double_el:
                    snprintf(buf, 100, el->format.c_str(), el->double_value);
                    break;
            }

            line += buf;
        }
        fprintf(out, "%s\n", line.c_str());
    }
}

///////////////////////////////////////////////////////////////////////////

float get_2_meaningful_digits(float val)
{
    float log = floor(log10(val));
    float factor = pow(10, log-1); // to save 2 digits
    float new_val = round( val / factor);
    new_val *= factor;
    return new_val;
}

inline unsigned long int ts_diff_now(struct timeval a)
{
    struct timeval b;

    gettimeofday(&b, NULL);
    unsigned long long aval = a.tv_sec * 1000000 + a.tv_usec;
    unsigned long long bval = b.tv_sec * 1000000 + b.tv_usec;

    return bval - aval;
}

inline timeval timeval_factorial_average(timeval a, timeval b, unsigned int weight)
{
    timeval tv;
    double factor = ((double)weight - 1) / weight;
    tv.tv_sec   = factor * a.tv_sec  + (double)b.tv_sec  / weight ;
    tv.tv_usec  = factor * a.tv_usec + (double)b.tv_usec / weight ;
    return (tv);
}

run_stats::one_second_stats::one_second_stats(unsigned int second)
{
    reset(second);
}

void run_stats::one_second_stats::reset(unsigned int second)
{
    m_second = second;
    m_bytes_get = m_bytes_set = 0;
    m_ops_get = m_ops_set = m_ops_wait = 0;
    m_get_hits = m_get_misses = 0;
    m_moved_get = m_moved_set = 0;
    m_total_get_latency = 0;
    m_total_set_latency = 0;
    m_total_wait_latency = 0;
}

void run_stats::one_second_stats::merge(const one_second_stats& other)
{
    m_bytes_get += other.m_bytes_get;
    m_bytes_set += other.m_bytes_set;
    m_ops_get += other.m_ops_get;
    m_ops_set += other.m_ops_set;
    m_ops_wait += other.m_ops_wait;
    m_get_hits += other.m_get_hits;
    m_get_misses += other.m_get_misses;
    m_moved_get += other.m_moved_get;
    m_moved_set += other.m_moved_set;
    m_total_get_latency += other.m_total_get_latency;
    m_total_set_latency += other.m_total_set_latency;
    m_total_wait_latency += other.m_total_wait_latency;
}

run_stats::totals::totals() :
        m_ops_sec_set(0),
        m_ops_sec_get(0),
        m_ops_sec_wait(0),
        m_ops_sec(0),
        m_hits_sec(0),
        m_misses_sec(0),
        m_moved_sec_set(0),
        m_moved_sec_get(0),
        m_moved_sec(0),
        m_bytes_sec_set(0),
        m_bytes_sec_get(0),
        m_bytes_sec(0),
        m_latency_set(0),
        m_latency_get(0),
        m_latency_wait(0),
        m_latency(0),
        m_bytes(0),
        m_ops_set(0),
        m_ops_get(0),
        m_ops_wait(0),
        m_ops(0)
{
}

void run_stats::totals::add(const run_stats::totals& other)
{
    m_ops_sec_set += other.m_ops_sec_set;
    m_ops_sec_get += other.m_ops_sec_get;
    m_ops_sec_wait += other.m_ops_sec_wait;
    m_ops_sec += other.m_ops_sec;
    m_hits_sec += other.m_hits_sec;
    m_misses_sec += other.m_misses_sec;
    m_moved_sec_set += other.m_moved_sec_set;
    m_moved_sec_get += other.m_moved_sec_get;
    m_moved_sec += other.m_moved_sec;
    m_bytes_sec_set += other.m_bytes_sec_set;
    m_bytes_sec_get += other.m_bytes_sec_get;
    m_bytes_sec += other.m_bytes_sec;
    m_latency_set += other.m_latency_set;
    m_latency_get += other.m_latency_get;
    m_latency_wait += other.m_latency_wait;
    m_latency += other.m_latency;
    m_bytes += other.m_bytes;
    m_ops_set += other.m_ops_set;
    m_ops_get += other.m_ops_get;
    m_ops_wait += other.m_ops_wait;
    m_ops += other.m_ops;
}

run_stats::run_stats() :
    m_cur_stats(0)
{
    memset(&m_start_time, 0, sizeof(m_start_time));
    memset(&m_end_time, 0, sizeof(m_end_time));
}

void run_stats::set_start_time(struct timeval* start_time)
{
    struct timeval tv;
    if (!start_time) {
        gettimeofday(&tv, NULL);
        start_time = &tv;
    }

    m_start_time = *start_time;
}

void run_stats::set_end_time(struct timeval* end_time)
{
    struct timeval tv;
    if (!end_time) {
        gettimeofday(&tv, NULL);
        end_time = &tv;
    }
    m_end_time = *end_time;
    m_stats.push_back(m_cur_stats);
}

void run_stats::roll_cur_stats(struct timeval* ts)
{
    unsigned int sec = ts_diff(m_start_time, *ts) / 1000000;
    if (sec > m_cur_stats.m_second) {
        m_stats.push_back(m_cur_stats);
        m_cur_stats.reset(sec);
    }
}

void run_stats::update_get_op(struct timeval* ts, unsigned int bytes, unsigned int latency, unsigned int hits, unsigned int misses)
{
    roll_cur_stats(ts);
    m_cur_stats.m_bytes_get += bytes;
    m_cur_stats.m_ops_get++;
    m_cur_stats.m_get_hits += hits;
    m_cur_stats.m_get_misses += misses;

    m_cur_stats.m_total_get_latency += latency;

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_get_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_bytes_set += bytes;
    m_cur_stats.m_ops_set++;

    m_cur_stats.m_total_set_latency += latency;

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_set_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_moved_get_op(struct timeval* ts, unsigned int bytes, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_bytes_get += bytes;
    m_cur_stats.m_ops_get++;
    m_cur_stats.m_moved_get++;

    m_cur_stats.m_total_get_latency += latency;

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_get_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_moved_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_bytes_set += bytes;
    m_cur_stats.m_ops_set++;
    m_cur_stats.m_moved_set++;

    m_cur_stats.m_total_set_latency += latency;

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_set_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_wait_op(struct timeval *ts, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_ops_wait++;

    m_cur_stats.m_total_wait_latency += latency;

    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_wait_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

unsigned int run_stats::get_duration(void)
{
    return m_cur_stats.m_second;
}

unsigned long int run_stats::get_duration_usec(void)
{
    if (!m_start_time.tv_sec)
        return 0;
    if (m_end_time.tv_sec > 0) {
        return ts_diff(m_start_time, m_end_time);
    } else {
        return ts_diff_now(m_start_time);
    }
}

unsigned long int run_stats::get_total_bytes(void)
{
    return m_totals.m_bytes;
}

unsigned long int run_stats::get_total_ops(void)
{
    return m_totals.m_ops;
}

unsigned long int run_stats::get_total_latency(void)
{
    return m_totals.m_latency;
}

#define AVERAGE(total, count) \
    ((unsigned int) ((count) > 0 ? (total) / (count) : 0))
#define USEC_FORMAT(value) \
    (value) / 1000000, (value) % 1000000

void run_stats::save_csv_one_sec(FILE *f,
                                 unsigned long int& total_get_ops,
                                 unsigned long int& total_set_ops,
                                 unsigned long int& total_wait_ops) {
    fprintf(f, "Per-Second Benchmark Data\n");
    fprintf(f, "Second,SET Requests,SET Average Latency,SET Total Bytes,"
               "GET Requests,GET Average Latency,GET Total Bytes,GET Misses, GET Hits,"
               "WAIT Requests,WAIT Average Latency\n");

    total_get_ops = 0;
    total_set_ops = 0;
    total_wait_ops = 0;

    for (std::vector<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        fprintf(f, "%u,%lu,%u.%06u,%lu,%lu,%u.%06u,%lu,%u,%u,%lu,%u.%06u\n",
                i->m_second,
                i->m_ops_set,
                USEC_FORMAT(AVERAGE(i->m_total_set_latency, i->m_ops_set)),
                i->m_bytes_set,
                i->m_ops_get,
                USEC_FORMAT(AVERAGE(i->m_total_get_latency, i->m_ops_get)),
                i->m_bytes_get,
                i->m_get_misses,
                i->m_get_hits,
                i->m_ops_wait,
                USEC_FORMAT(AVERAGE(i->m_total_wait_latency, i->m_ops_wait)));

        total_get_ops += i->m_ops_get;
        total_set_ops += i->m_ops_set;
        total_wait_ops += i->m_ops_wait;
    }
}

void run_stats::save_csv_one_sec_cluster(FILE *f,
                                         unsigned long int& total_get_ops,
                                         unsigned long int& total_set_ops,
                                         unsigned long int& total_wait_ops) {
    fprintf(f, "Per-Second Benchmark Data\n");
    fprintf(f, "Second,SET Requests,SET Average Latency,SET Total Bytes,SET Moved,"
               "GET Requests,GET Average Latency,GET Total Bytes,GET Misses,GET Hits,GET Moved,"
               "WAIT Requests,WAIT Average Latency\n");

    total_get_ops = 0;
    total_set_ops = 0;
    total_wait_ops = 0;

    for (std::vector<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        fprintf(f, "%u,%lu,%u.%06u,%lu,%u,%lu,%u.%06u,%lu,%u,%u,%u,%lu,%u.%06u\n",
                i->m_second,
                i->m_ops_set,
                USEC_FORMAT(AVERAGE(i->m_total_set_latency, i->m_ops_set)),
                i->m_bytes_set,
                i->m_moved_set,
                i->m_ops_get,
                USEC_FORMAT(AVERAGE(i->m_total_get_latency, i->m_ops_get)),
                i->m_bytes_get,
                i->m_get_misses,
                i->m_get_hits,
                i->m_moved_get,
                i->m_ops_wait,
                USEC_FORMAT(AVERAGE(i->m_total_wait_latency, i->m_ops_wait)));

        total_get_ops += i->m_ops_get;
        total_set_ops += i->m_ops_set;
        total_wait_ops += i->m_ops_wait;
    }
}

bool run_stats::save_csv(const char *filename, bool cluster_mode)
{
    FILE *f = fopen(filename, "w");
    if (!f) {
        perror(filename);
        return false;
    }

    unsigned long int total_get_ops;
    unsigned long int total_set_ops;
    unsigned long int total_wait_ops;

    // save per second data
    if (cluster_mode) {
        save_csv_one_sec_cluster(f, total_get_ops, total_set_ops, total_wait_ops);
    } else {
        save_csv_one_sec(f, total_get_ops, total_set_ops, total_wait_ops);
    }

    double total_count_float = 0;
    fprintf(f, "\n" "Full-Test GET Latency\n");
    fprintf(f, "Latency (<= msec),Percent\n");
    for ( latency_map_itr it = m_get_latency_map.begin() ; it != m_get_latency_map.end() ; it++ ) {
        total_count_float += it->second;
        fprintf(f, "%8.3f,%.2f\n", it->first, total_count_float / total_get_ops * 100);
    }

    total_count_float = 0;
    fprintf(f, "\n" "Full-Test SET Latency\n");
    fprintf(f, "Latency (<= msec),Percent\n");
    for ( latency_map_itr it = m_set_latency_map.begin(); it != m_set_latency_map.end() ; it++ ) {
        total_count_float += it->second;
        fprintf(f, "%8.3f,%.2f\n", it->first, total_count_float / total_set_ops * 100);
    }

    total_count_float = 0;
    fprintf(f, "\n" "Full-Test WAIT Latency\n");
    fprintf(f, "Latency (<= msec),Percent\n");
    for ( latency_map_itr it = m_wait_latency_map.begin(); it != m_wait_latency_map.end() ; it++ ) {
        total_count_float += it->second;
        fprintf(f, "%8.3f,%.2f\n", it->first, total_count_float / total_wait_ops * 100);
    }

    fclose(f);
    return true;
}

void run_stats::debug_dump(void)
{
    benchmark_debug_log("run_stats: start_time={%u,%u} end_time={%u,%u}\n",
                        m_start_time.tv_sec, m_start_time.tv_usec,
                        m_end_time.tv_sec, m_end_time.tv_usec);

    for (std::vector<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        benchmark_debug_log("  %u: get latency=%u.%ums, set latency=%u.%ums, wait latency=%u.%ums"
                            "m_ops_set/get/wait=%u/%u/%u, m_bytes_set/get=%u/%u, m_get_hit/miss=%u/%u\n",
                            i->m_second,
                            USEC_FORMAT(AVERAGE(i->m_total_get_latency, i->m_ops_get)),
                            USEC_FORMAT(AVERAGE(i->m_total_set_latency, i->m_ops_set)),
                            USEC_FORMAT(AVERAGE(i->m_total_wait_latency, i->m_ops_wait)),
                            i->m_ops_set,
                            i->m_ops_get,
                            i->m_ops_wait,
                            i->m_bytes_set,
                            i->m_bytes_get,
                            i->m_get_hits,
                            i->m_get_misses);
    }


    for( latency_map_itr it = m_get_latency_map.begin() ; it != m_get_latency_map.end() ; it++) {
        if (it->second)
            benchmark_debug_log("  GET <= %u msec: %u\n", it->first, it->second);
    }
    for(  latency_map_itr it = m_set_latency_map.begin() ; it != m_set_latency_map.end() ; it++) {
        if (it->second)
            benchmark_debug_log("  SET <= %u msec: %u\n", it->first, it->second);
    }
    for(  latency_map_itr it = m_wait_latency_map.begin() ; it != m_wait_latency_map.end() ; it++) {
        if (it->second)
            benchmark_debug_log("  WAIT <= %u msec: %u\n", it->first, it->second);
    }
}

bool one_second_stats_predicate(const run_stats::one_second_stats& a, const run_stats::one_second_stats& b)
{
    return a.m_second < b.m_second;
}

void run_stats::aggregate_average(const std::vector<run_stats>& all_stats)
{
    for (std::vector<run_stats>::const_iterator i = all_stats.begin();
         i != all_stats.end(); i++) {
        totals i_totals;
        i->summarize(i_totals);
        m_totals.add(i_totals);

        // aggregate latency data

        for (latency_map_itr_const it = i->m_get_latency_map.begin() ; it != i->m_get_latency_map.end() ; it++) {
            m_get_latency_map[it->first] += it->second;
        }
        for (latency_map_itr_const it = i->m_set_latency_map.begin() ; it != i->m_set_latency_map.end() ; it++) {
            m_set_latency_map[it->first] += it->second;
        }
        for (latency_map_itr_const it = i->m_wait_latency_map.begin() ; it != i->m_wait_latency_map.end() ; it++) {
            m_wait_latency_map[it->first] += it->second;
        }
    }
    m_totals.m_ops_sec_set /= all_stats.size();
    m_totals.m_ops_sec_get /= all_stats.size();
    m_totals.m_ops_sec_wait /= all_stats.size();
    m_totals.m_ops_sec /= all_stats.size();
    m_totals.m_hits_sec /= all_stats.size();
    m_totals.m_misses_sec /= all_stats.size();
    m_totals.m_moved_sec_set /= all_stats.size();
    m_totals.m_moved_sec_get /= all_stats.size();
    m_totals.m_moved_sec /= all_stats.size();
    m_totals.m_bytes_sec_set /= all_stats.size();
    m_totals.m_bytes_sec_get /= all_stats.size();
    m_totals.m_bytes_sec /= all_stats.size();
    m_totals.m_latency_set /= all_stats.size();
    m_totals.m_latency_get /= all_stats.size();
    m_totals.m_latency_wait /= all_stats.size();
    m_totals.m_latency /= all_stats.size();

}

void run_stats::merge(const run_stats& other, int iteration)
{
    bool new_stats = false;

    m_start_time = timeval_factorial_average( m_start_time, other.m_start_time, iteration );
    m_end_time =   timeval_factorial_average( m_end_time,   other.m_end_time,   iteration );

    // aggregate the one_second_stats vectors. this is not efficient
    // but it's not really important (small numbers, not realtime)
    for (std::vector<one_second_stats>::const_iterator other_i = other.m_stats.begin();
         other_i != other.m_stats.end(); other_i++) {

        // find ours
        bool merged = false;
        for (std::vector<one_second_stats>::iterator i = m_stats.begin();
             i != m_stats.end(); i++) {
            if (i->m_second == other_i->m_second) {
                i->merge(*other_i);
                merged = true;
                break;
            }
        }

        if (!merged) {
            m_stats.push_back(*other_i);
            new_stats = true;
        }
    }

    if (new_stats) {
        sort(m_stats.begin(), m_stats.end(), one_second_stats_predicate);
    }

    // aggregate totals
    m_totals.m_bytes += other.m_totals.m_bytes;
    m_totals.m_ops += other.m_totals.m_ops;

    // aggregate latency data
    for (latency_map_itr_const it = other.m_get_latency_map.begin() ; it != other.m_get_latency_map.end() ; it++) {
        m_get_latency_map[it->first] += it->second;
    }
    for (latency_map_itr_const it = other.m_set_latency_map.begin() ; it != other.m_set_latency_map.end() ; it++) {
        m_set_latency_map[it->first] += it->second;
    }
    for (latency_map_itr_const it = other.m_wait_latency_map.begin() ; it != other.m_wait_latency_map.end() ; it++) {
        m_wait_latency_map[it->first] += it->second;
    }
}

void run_stats::summarize(totals& result) const
{
    // aggregate all one_second_stats
    one_second_stats totals(0);
    for (std::vector<one_second_stats>::const_iterator i = m_stats.begin();
         i != m_stats.end(); i++) {
        totals.merge(*i);
    }

    unsigned long int test_duration_usec = ts_diff(m_start_time, m_end_time);

    result.m_ops_set = totals.m_ops_set;
    result.m_ops_get = totals.m_ops_get;
    result.m_ops_wait = totals.m_ops_wait;

    result.m_ops = totals.m_ops_get + totals.m_ops_set + totals.m_ops_wait;
    result.m_bytes = totals.m_bytes_get + totals.m_bytes_set;

    result.m_ops_sec_set = (double) totals.m_ops_set / test_duration_usec * 1000000;
    if (totals.m_ops_set > 0) {
        result.m_latency_set = (double) (totals.m_total_set_latency / totals.m_ops_set) / 1000;
    } else {
        result.m_latency_set = 0;
    }
    result.m_bytes_sec_set = (totals.m_bytes_set / 1024.0) / test_duration_usec * 1000000;
    result.m_moved_sec_set = (double) totals.m_moved_set / test_duration_usec * 1000000;

    result.m_ops_sec_get = (double) totals.m_ops_get / test_duration_usec * 1000000;
    if (totals.m_ops_get > 0) {
        result.m_latency_get = (double) (totals.m_total_get_latency / totals.m_ops_get) / 1000;
    } else {
        result.m_latency_get = 0;
    }
    result.m_bytes_sec_get = (totals.m_bytes_get / 1024.0) / test_duration_usec * 1000000;
    result.m_hits_sec = (double) totals.m_get_hits / test_duration_usec * 1000000;
    result.m_misses_sec = (double) totals.m_get_misses / test_duration_usec * 1000000;
    result.m_moved_sec_get = (double) totals.m_moved_get / test_duration_usec * 1000000;

    result.m_ops_sec_wait =  (double) totals.m_ops_wait / test_duration_usec * 1000000;
    if (totals.m_ops_wait > 0) {
        result.m_latency_wait = (double) (totals.m_total_wait_latency / totals.m_ops_wait) / 1000;
    } else {
        result.m_latency_wait = 0;
    }

    result.m_ops_sec = (double) result.m_ops / test_duration_usec * 1000000;
    if (result.m_ops > 0) {
        result.m_latency = (double) ((totals.m_total_get_latency + totals.m_total_set_latency + totals.m_total_wait_latency) / result.m_ops) / 1000;
    } else {
        result.m_latency = 0;
    }
    result.m_bytes_sec = (result.m_bytes / 1024.0) / test_duration_usec * 1000000;
    result.m_moved_sec = (double) (totals.m_moved_get + totals.m_moved_set) / test_duration_usec * 1000000;
}

void result_print_to_json(json_handler * jsonhandler, const char * type, double ops, double hits, double miss, double moved, double latency, double kbs)
{
    if (jsonhandler != NULL){ // Added for double verification in case someone accidently send NULL.
        jsonhandler->open_nesting(type);
        jsonhandler->write_obj("Ops/sec","%.2f", ops);
        jsonhandler->write_obj("Hits/sec","%.2f", hits);
        jsonhandler->write_obj("Misses/sec","%.2f", miss);

        if (moved >= 0)
            jsonhandler->write_obj("Moved/sec","%.2f", moved);

        jsonhandler->write_obj("Latency","%.2f", latency);
        jsonhandler->write_obj("KB/sec","%.2f", kbs);
        jsonhandler->close_nesting();
    }
}

void histogram_print(FILE * out, json_handler * jsonhandler, const char * type, float msec, float percent)
{
    fprintf(out, "%-6s %8.3f %12.2f\n", type, msec, percent);
    if (jsonhandler != NULL){
        jsonhandler->open_nesting(NULL);
        jsonhandler->write_obj("<=msec","%.3f", msec);
        jsonhandler->write_obj("percent","%.2f", percent);
        jsonhandler->close_nesting();
    }
}

void run_stats::print(FILE *out, bool histogram, const char * header/*=NULL*/,  json_handler * jsonhandler/*=NULL*/, bool cluster_mode/*=false*/)
{
    // aggregate all one_second_stats; we do this only if we have
    // one_second_stats, otherwise it means we're probably printing previously
    // aggregated data
    if (m_stats.size() > 0) {
        summarize(m_totals);
    }

    table_el el;
    table_column column;
    output_table table;

    // Type column
    column.elements.push_back(*el.init_str("%-6s ", "Type"));
    column.elements.push_back(*el.init_str("%s", "------------"));
    column.elements.push_back(*el.init_str("%-6s ", "Sets"));
    column.elements.push_back(*el.init_str("%-6s ", "Gets"));
    column.elements.push_back(*el.init_str("%-6s ", "Waits"));
    column.elements.push_back(*el.init_str("%-6s ", "Totals"));

    table.add_column(column);
    column.elements.clear();

    // Ops/sec column
    column.elements.push_back(*el.init_str("%12s ", "Ops/sec"));
    column.elements.push_back(*el.init_str("%s", "------------"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ops_sec_set));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ops_sec_get));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ops_sec_wait));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ops_sec));

    table.add_column(column);
    column.elements.clear();

    // Hits/sec column
    column.elements.push_back(*el.init_str("%12s ", "Hits/sec"));
    column.elements.push_back(*el.init_str("%s", "------------"));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_hits_sec));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_hits_sec));

    table.add_column(column);
    column.elements.clear();

    // Misses/sec column
    column.elements.push_back(*el.init_str("%12s ", "Misses/sec"));
    column.elements.push_back(*el.init_str("%s", "------------"));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_misses_sec));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_misses_sec));

    table.add_column(column);
    column.elements.clear();

    // Moved information relevant only for cluster mode
    if (cluster_mode) {
        // Moved/sec column
        column.elements.push_back(*el.init_str("%12s ", "Moved/sec"));
        column.elements.push_back(*el.init_str("%s", "------------"));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_moved_sec_set));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_moved_sec_get));
        column.elements.push_back(*el.init_str("%12s ", "---"));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_moved_sec));

        table.add_column(column);
        column.elements.clear();
    }

    // Latency column
    column.elements.push_back(*el.init_str("%12s ", "Latency"));
    column.elements.push_back(*el.init_str("%s", "------------"));
    column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_latency_set));
    column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_latency_get));
    column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_latency_wait));
    column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_latency));

    table.add_column(column);
    column.elements.clear();

    // KB/sec column
    column.elements.push_back(*el.init_str("%12s ", "KB/sec"));
    column.elements.push_back(*el.init_str("%s", "------------"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_bytes_sec_set));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_bytes_sec_get));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_bytes_sec));

    table.add_column(column);
    column.elements.clear();

    // print results
    table.print(out, header);

    ////////////////////////////////////////
    // JSON print handling
    // ------------------
    if (jsonhandler != NULL){

        if (header != NULL) {
            jsonhandler->open_nesting(header);
        } else {
            jsonhandler->open_nesting("UNKNOWN STATS");
        }

        result_print_to_json(jsonhandler, "Sets",m_totals.m_ops_sec_set,
                             0.0,
                             0.0,
                             cluster_mode ? m_totals.m_moved_sec_set : -1,
                             m_totals.m_latency_set,
                             m_totals.m_bytes_sec_set);
        result_print_to_json(jsonhandler,"Gets",m_totals.m_ops_sec_get,
                             m_totals.m_hits_sec,
                             m_totals.m_misses_sec,
                             cluster_mode ? m_totals.m_moved_sec_get : -1,
                             m_totals.m_latency_get,
                             m_totals.m_bytes_sec_get);
        result_print_to_json(jsonhandler,"Waits",m_totals.m_ops_sec_wait,
                             0.0,
                             0.0,
                             cluster_mode ? 0.0 : -1,
                             m_totals.m_latency_wait,
                             0.0);
        result_print_to_json(jsonhandler,"Totals",m_totals.m_ops_sec,
                             m_totals.m_hits_sec,
                             m_totals.m_misses_sec,
                             cluster_mode ? m_totals.m_moved_sec : -1,
                             m_totals.m_latency,
                             m_totals.m_bytes_sec);
    }

    if (histogram)
    {
        fprintf(out,
                "\n\n"
                "Request Latency Distribution\n"
                "%-6s %12s %12s\n"
                "------------------------------------------------------------------------\n",
                "Type", "<= msec   ", "Percent");

        unsigned long int total_count = 0;
        // SETs
        // ----
        if (jsonhandler != NULL){ jsonhandler->open_nesting("SET",NESTED_ARRAY);}
        for( latency_map_itr_const it = m_set_latency_map.begin() ; it != m_set_latency_map.end() ; it++) {
            total_count += it->second;
            histogram_print(out, jsonhandler, "SET",it->first,(double) total_count / m_totals.m_ops_set * 100);
        }
        if (jsonhandler != NULL){ jsonhandler->close_nesting();}
        fprintf(out, "---\n");
        // GETs
        // ----
        total_count = 0;
        if (jsonhandler != NULL){ jsonhandler->open_nesting("GET",NESTED_ARRAY);}
        for( latency_map_itr_const it = m_get_latency_map.begin() ; it != m_get_latency_map.end() ; it++) {
            total_count += it->second;
            histogram_print(out, jsonhandler, "GET",it->first,(double) total_count / m_totals.m_ops_get * 100);
        }
        if (jsonhandler != NULL){ jsonhandler->close_nesting();}
        fprintf(out, "---\n");
        // WAITs
        // ----
        total_count = 0;
        if (jsonhandler != NULL){ jsonhandler->open_nesting("WAIT",NESTED_ARRAY);}
        for( latency_map_itr_const it = m_wait_latency_map.begin() ; it != m_wait_latency_map.end() ; it++) {
            total_count += it->second;
            histogram_print(out, jsonhandler, "WAIT",it->first,(double) total_count / m_totals.m_ops_wait * 100);
        }
        if (jsonhandler != NULL){ jsonhandler->close_nesting();}
    }
    // This close_nesting closes either:
    //      jsonhandler->open_nesting(header); or
    //      jsonhandler->open_nesting("UNKNOWN STATS");
    //      From the top (beginning of function).
    if (jsonhandler != NULL){ jsonhandler->close_nesting();}
}


