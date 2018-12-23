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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <math.h>
#include <algorithm>

#ifdef HAVE_ASSERT_H
#include <assert.h>

#endif

#include "run_stats.h"
#include "memtier_benchmark.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

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
        char buf[100];
        size_t header_size = columns[i].column_size + 1;
        memset(buf, '=', header_size);
        buf[header_size] = '\0';

        fprintf(out,"%s", buf);
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
    m_cur_stats.m_get_cmd.update_op(bytes, latency, hits, misses);

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_get_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_set_cmd.update_op(bytes, latency);

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_set_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_moved_get_op(struct timeval* ts, unsigned int bytes, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_get_cmd.update_moved_op(bytes, latency);

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_get_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_moved_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_set_cmd.update_moved_op(bytes, latency);

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_set_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_ask_get_op(struct timeval* ts, unsigned int bytes, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_get_cmd.update_ask_op(bytes, latency);

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_get_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_ask_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_set_cmd.update_ask_op(bytes, latency);

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_set_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_wait_op(struct timeval *ts, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_wait_cmd.update_op(0, latency);

    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_wait_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_aribitrary_op(struct timeval* ts, unsigned int bytes, unsigned int latency) {
    roll_cur_stats(ts);
    m_cur_stats.m_ar_cmd.update_op(bytes, latency);

    m_totals.m_bytes += bytes;
    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_ar_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
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
               "GET Requests,GET Average Latency,GET Total Bytes,GET Misses,GET Hits,"
               "WAIT Requests,WAIT Average Latency\n");

    total_get_ops = 0;
    total_set_ops = 0;
    total_wait_ops = 0;

    for (std::vector<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        fprintf(f, "%u,%lu,%u.%06u,%lu,%lu,%u.%06u,%lu,%u,%u,%lu,%u.%06u\n",
                i->m_second,
                i->m_set_cmd.m_ops,
                USEC_FORMAT(AVERAGE(i->m_set_cmd.m_total_latency, i->m_set_cmd.m_ops)),
                i->m_set_cmd.m_bytes,
                i->m_get_cmd.m_ops,
                USEC_FORMAT(AVERAGE(i->m_get_cmd.m_total_latency, i->m_get_cmd.m_ops)),
                i->m_get_cmd.m_bytes,
                i->m_get_cmd.m_misses,
                i->m_get_cmd.m_hits,
                i->m_wait_cmd.m_ops,
                USEC_FORMAT(AVERAGE(i->m_wait_cmd.m_total_latency, i->m_wait_cmd.m_ops)));

        total_set_ops += i->m_set_cmd.m_ops;
        total_get_ops += i->m_get_cmd.m_ops;
        total_wait_ops += i->m_wait_cmd.m_ops;
    }
}

void run_stats::save_csv_one_sec_cluster(FILE *f) {
    fprintf(f, "\nPer-Second Benchmark Cluster Data\n");
    fprintf(f, "Second,SET Moved,SET Ask,GET Moved,GET Ask\n");

    for (std::vector<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        fprintf(f, "%u,%u,%u,%u,%u\n",
                i->m_second,
                i->m_set_cmd.m_moved,
                i->m_set_cmd.m_ask,
                i->m_get_cmd.m_moved,
                i->m_get_cmd.m_ask);
    }
}

void run_stats::save_ar_one_sec(FILE *f,
                                std::string ar_cmd_name,
                                unsigned long int& total_ar_ops) {
    fprintf(f, "Per-Second Benchmark Arbitrary Data\n");
    fprintf(f, "Second,%s Requests,%s Average Latency,%s Total Bytes\n",
            ar_cmd_name.c_str(), ar_cmd_name.c_str(), ar_cmd_name.c_str());

    total_ar_ops = 0;

    for (std::vector<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        fprintf(f, "%u,%lu,%u.%06u,%lu\n",
                i->m_second,
                i->m_ar_cmd.m_ops,
                USEC_FORMAT(AVERAGE(i->m_ar_cmd.m_total_latency, i->m_ar_cmd.m_ops)),
                i->m_ar_cmd.m_bytes);

        total_ar_ops += i->m_ar_cmd.m_ops;
    }
}

bool run_stats::save_csv(const char *filename, bool cluster_mode, std::string ar_cmd_name)
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
    save_csv_one_sec(f, total_get_ops, total_set_ops, total_wait_ops);

    // save latency data
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

    // cluster mode data
    if (cluster_mode) {
        save_csv_one_sec_cluster(f);
    }

    // arbitrary command data
    if (ar_cmd_name.length() > 0) {
        // format command name
        std::transform(ar_cmd_name.begin(), ar_cmd_name.end(), ar_cmd_name.begin(), ::toupper);

        unsigned long int total_ar_ops;

        // save per second data
        save_ar_one_sec(f, ar_cmd_name, total_ar_ops);

        // save latency data
        total_count_float = 0;
        fprintf(f, "\n" "Full-Test %s Latency\n", ar_cmd_name.c_str());
        fprintf(f, "Latency (<= msec),Percent\n");
        for ( latency_map_itr it = m_ar_latency_map.begin(); it != m_ar_latency_map.end() ; it++ ) {
            total_count_float += it->second;
            fprintf(f, "%8.3f,%.2f\n", it->first, total_count_float / total_ar_ops * 100);
        }
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
                            USEC_FORMAT(AVERAGE(i->m_get_cmd.m_total_latency, i->m_get_cmd.m_ops)),
                            USEC_FORMAT(AVERAGE(i->m_set_cmd.m_total_latency, i->m_set_cmd.m_ops)),
                            USEC_FORMAT(AVERAGE(i->m_wait_cmd.m_total_latency, i->m_wait_cmd.m_ops)),
                            i->m_set_cmd.m_ops,
                            i->m_get_cmd.m_ops,
                            i->m_wait_cmd.m_ops,
                            i->m_set_cmd.m_bytes,
                            i->m_get_cmd.m_bytes,
                            i->m_get_cmd.m_hits,
                            i->m_get_cmd.m_misses);
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

bool one_second_stats_predicate(const one_second_stats& a, const one_second_stats& b)
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
        for (latency_map_itr_const it = i->m_ar_latency_map.begin() ; it != i->m_ar_latency_map.end() ; it++) {
            m_ar_latency_map[it->first] += it->second;
        }
    }

    m_totals.m_set_cmd.aggregate_average(all_stats.size());
    m_totals.m_get_cmd.aggregate_average(all_stats.size());
    m_totals.m_wait_cmd.aggregate_average(all_stats.size());
    m_totals.m_ar_cmd.aggregate_average(all_stats.size());
    m_totals.m_ops_sec /= all_stats.size();
    m_totals.m_hits_sec /= all_stats.size();
    m_totals.m_misses_sec /= all_stats.size();
    m_totals.m_moved_sec /= all_stats.size();
    m_totals.m_ask_sec /= all_stats.size();
    m_totals.m_bytes_sec /= all_stats.size();
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
    for (latency_map_itr_const it = other.m_ar_latency_map.begin() ; it != other.m_ar_latency_map.end() ; it++) {
        m_ar_latency_map[it->first] += it->second;
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

    // total ops, bytes
    result.m_ops = totals.m_set_cmd.m_ops + totals.m_get_cmd.m_ops + totals.m_wait_cmd.m_ops + totals.m_ar_cmd.m_ops;
    result.m_bytes = totals.m_set_cmd.m_bytes + totals.m_get_cmd.m_bytes + totals.m_ar_cmd.m_bytes;

    // cmd/sec
    result.m_set_cmd.summarize(totals.m_set_cmd, test_duration_usec);
    result.m_get_cmd.summarize(totals.m_get_cmd, test_duration_usec);
    result.m_wait_cmd.summarize(totals.m_wait_cmd, test_duration_usec);
    result.m_ar_cmd.summarize(totals.m_ar_cmd, test_duration_usec);

    // hits,misses / sec
    result.m_hits_sec = (double) totals.m_get_cmd.m_hits / test_duration_usec * 1000000;
    result.m_misses_sec = (double) totals.m_get_cmd.m_misses / test_duration_usec * 1000000;

    // total/sec
    result.m_ops_sec = (double) result.m_ops / test_duration_usec * 1000000;
    if (result.m_ops > 0) {
        result.m_latency = (double) ((totals.m_set_cmd.m_total_latency +
                                      totals.m_get_cmd.m_total_latency +
                                      totals.m_wait_cmd.m_total_latency +
                                      totals.m_ar_cmd.m_total_latency) /
                                     result.m_ops) /
                                    1000;
    } else {
        result.m_latency = 0;
    }
    result.m_bytes_sec = (result.m_bytes / 1024.0) / test_duration_usec * 1000000;
    result.m_moved_sec = (double) (totals.m_set_cmd.m_moved + totals.m_get_cmd.m_moved) / test_duration_usec * 1000000;
    result.m_ask_sec = (double) (totals.m_set_cmd.m_ask + totals.m_get_cmd.m_ask) / test_duration_usec * 1000000;
}

void result_print_to_json(json_handler * jsonhandler, const char * type, double ops,
                          double hits, double miss, double moved, double ask, double latency, double kbs)
{
    if (jsonhandler != NULL){ // Added for double verification in case someone accidently send NULL.
        jsonhandler->open_nesting(type);
        jsonhandler->write_obj("Ops/sec","%.2f", ops);
        jsonhandler->write_obj("Hits/sec","%.2f", hits);
        jsonhandler->write_obj("Misses/sec","%.2f", miss);

        if (moved >= 0)
            jsonhandler->write_obj("MOVED/sec","%.2f", moved);

        if (ask >= 0)
            jsonhandler->write_obj("ASK/sec","%.2f", ask);

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

void run_stats::print(FILE *out, bool histogram,
                      const char * header/*=NULL*/,  json_handler * jsonhandler/*=NULL*/,
                      bool cluster_mode/*=false*/, std::string ar_cmd_name /*=""*/)
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

    // we print either set/get/wait or arbitrary command statistics
    bool print_ar_cmd_stats = ar_cmd_name.length() > 0;

    // Type column
    column.column_size = MAX(6, ar_cmd_name.length()) + 1;
    assert(column.column_size < 100 && "command name too long");

    // set enough space according to size of command name
    char buf[100];
    snprintf(buf, 100, "%%-%us ", column.column_size);
    std::string type_col_format(buf);
    memset(buf, '-', column.column_size + 1);
    buf[column.column_size + 1] = '\0';

    column.elements.push_back(*el.init_str(type_col_format, "Type"));
    column.elements.push_back(*el.init_str("%s", buf));
    if (print_ar_cmd_stats) {
        // format command name
        std::transform(ar_cmd_name.begin(), ar_cmd_name.end(), ar_cmd_name.begin(), ::tolower);
        ar_cmd_name[0] = static_cast<char>(toupper(ar_cmd_name[0]));
        ar_cmd_name.append("s");

        column.elements.push_back(*el.init_str(type_col_format, ar_cmd_name));
    } else {
        column.elements.push_back(*el.init_str(type_col_format, "Sets"));
        column.elements.push_back(*el.init_str(type_col_format, "Gets"));
        column.elements.push_back(*el.init_str(type_col_format, "Waits"));
    }
    column.elements.push_back(*el.init_str(type_col_format, "Totals"));

    table.add_column(column);
    column.elements.clear();
    column.column_size = 12;

    // Ops/sec column
    column.elements.push_back(*el.init_str("%12s ", "Ops/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));
    if (print_ar_cmd_stats) {
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ar_cmd.m_ops_sec));
    } else {
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_set_cmd.m_ops_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_get_cmd.m_ops_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_wait_cmd.m_ops_sec));
    }
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ops_sec));

    table.add_column(column);
    column.elements.clear();

    // Hits/sec column
    column.elements.push_back(*el.init_str("%12s ", "Hits/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));
    if (print_ar_cmd_stats) {
        column.elements.push_back(*el.init_str("%12s ", "---"));
    } else {
        column.elements.push_back(*el.init_str("%12s ", "---"));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_hits_sec));
        column.elements.push_back(*el.init_str("%12s ", "---"));
    }
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_hits_sec));

    table.add_column(column);
    column.elements.clear();

    // Misses/sec column
    column.elements.push_back(*el.init_str("%12s ", "Misses/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));
    if (print_ar_cmd_stats) {
        column.elements.push_back(*el.init_str("%12s ", "---"));
    } else {
        column.elements.push_back(*el.init_str("%12s ", "---"));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_misses_sec));
        column.elements.push_back(*el.init_str("%12s ", "---"));
    }
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_misses_sec));

    table.add_column(column);
    column.elements.clear();

    // Moved & ASK information relevant only for cluster mode
    if (cluster_mode) {
        // Moved/sec column
        column.elements.push_back(*el.init_str("%12s ", "MOVED/sec"));
        column.elements.push_back(*el.init_str("%s", "-------------"));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_set_cmd.m_moved_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_get_cmd.m_moved_sec));
        column.elements.push_back(*el.init_str("%12s ", "---"));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_moved_sec));

        table.add_column(column);
        column.elements.clear();

        // ASK/sec column
        column.elements.push_back(*el.init_str("%12s ", "ASK/sec"));
        column.elements.push_back(*el.init_str("%s", "-------------"));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_set_cmd.m_ask_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_get_cmd.m_ask_sec));
        column.elements.push_back(*el.init_str("%12s ", "---"));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ask_sec));

        table.add_column(column);
        column.elements.clear();
    }

    // Latency column
    column.elements.push_back(*el.init_str("%12s ", "Latency"));
    column.elements.push_back(*el.init_str("%s", "-------------"));
    if (print_ar_cmd_stats) {
        column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_ar_cmd.m_latency));
    } else {
        column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_set_cmd.m_latency));
        column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_get_cmd.m_latency));
        column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_wait_cmd.m_latency));
    }
    column.elements.push_back(*el.init_double("%12.05f ", m_totals.m_latency));

    table.add_column(column);
    column.elements.clear();

    // KB/sec column
    column.elements.push_back(*el.init_str("%12s ", "KB/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));
    if (print_ar_cmd_stats) {
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ar_cmd.m_bytes_sec));
    } else {
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_set_cmd.m_bytes_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_get_cmd.m_bytes_sec));
        column.elements.push_back(*el.init_str("%12s ", "---"));
    }
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

        if (print_ar_cmd_stats) {
            result_print_to_json(jsonhandler, ar_cmd_name.c_str(),m_totals.m_ar_cmd.m_ops_sec,
                                 0.0,
                                 0.0,
                                 cluster_mode ? m_totals.m_ar_cmd.m_moved_sec : -1,
                                 cluster_mode ? m_totals.m_ar_cmd.m_ask_sec : -1,
                                 m_totals.m_ar_cmd.m_latency,
                                 m_totals.m_ar_cmd.m_bytes_sec);
        } else {
            result_print_to_json(jsonhandler, "Sets",m_totals.m_set_cmd.m_ops_sec,
                                 0.0,
                                 0.0,
                                 cluster_mode ? m_totals.m_set_cmd.m_moved_sec : -1,
                                 cluster_mode ? m_totals.m_set_cmd.m_ask_sec : -1,
                                 m_totals.m_set_cmd.m_latency,
                                 m_totals.m_set_cmd.m_bytes_sec);
            result_print_to_json(jsonhandler,"Gets",m_totals.m_get_cmd.m_ops_sec,
                                 m_totals.m_hits_sec,
                                 m_totals.m_misses_sec,
                                 cluster_mode ? m_totals.m_get_cmd.m_moved_sec : -1,
                                 cluster_mode ? m_totals.m_get_cmd.m_ask_sec : -1,
                                 m_totals.m_get_cmd.m_latency,
                                 m_totals.m_get_cmd.m_bytes_sec);
            result_print_to_json(jsonhandler,"Waits",m_totals.m_wait_cmd.m_ops_sec,
                                 0.0,
                                 0.0,
                                 cluster_mode ? 0.0 : -1,
                                 cluster_mode ? 0.0 : -1,
                                 m_totals.m_wait_cmd.m_latency,
                                 0.0);
        }

        result_print_to_json(jsonhandler,"Totals",m_totals.m_ops_sec,
                             m_totals.m_hits_sec,
                             m_totals.m_misses_sec,
                             cluster_mode ? m_totals.m_moved_sec : -1,
                             cluster_mode ? m_totals.m_ask_sec : -1,
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
        if (print_ar_cmd_stats) {
            // format command name
            ar_cmd_name.erase(ar_cmd_name.end()-1);
            std::transform(ar_cmd_name.begin(), ar_cmd_name.end(), ar_cmd_name.begin(), ::toupper);

            // Arbitrary command
            // ----
            if (jsonhandler != NULL){ jsonhandler->open_nesting(ar_cmd_name.c_str(),NESTED_ARRAY);}
            for( latency_map_itr_const it = m_ar_latency_map.begin() ; it != m_ar_latency_map.end() ; it++) {
                total_count += it->second;
                histogram_print(out, jsonhandler, ar_cmd_name.c_str(),it->first,(double) total_count / m_totals.m_ar_cmd.m_ops * 100);
            }
            if (jsonhandler != NULL){ jsonhandler->close_nesting();}
        } else {
            // SETs
            // ----
            if (jsonhandler != NULL){ jsonhandler->open_nesting("SET",NESTED_ARRAY);}
            for( latency_map_itr_const it = m_set_latency_map.begin() ; it != m_set_latency_map.end() ; it++) {
                total_count += it->second;
                histogram_print(out, jsonhandler, "SET",it->first,(double) total_count / m_totals.m_set_cmd.m_ops * 100);
            }
            if (jsonhandler != NULL){ jsonhandler->close_nesting();}
            fprintf(out, "---\n");
            // GETs
            // ----
            total_count = 0;
            if (jsonhandler != NULL){ jsonhandler->open_nesting("GET",NESTED_ARRAY);}
            for( latency_map_itr_const it = m_get_latency_map.begin() ; it != m_get_latency_map.end() ; it++) {
                total_count += it->second;
                histogram_print(out, jsonhandler, "GET",it->first,(double) total_count / m_totals.m_get_cmd.m_ops * 100);
            }
            if (jsonhandler != NULL){ jsonhandler->close_nesting();}
            fprintf(out, "---\n");
            // WAITs
            // ----
            total_count = 0;
            if (jsonhandler != NULL){ jsonhandler->open_nesting("WAIT",NESTED_ARRAY);}
            for( latency_map_itr_const it = m_wait_latency_map.begin() ; it != m_wait_latency_map.end() ; it++) {
                total_count += it->second;
                histogram_print(out, jsonhandler, "WAIT",it->first,(double) total_count / m_totals.m_wait_cmd.m_ops * 100);
            }
            if (jsonhandler != NULL){ jsonhandler->close_nesting();}
        }


    }

    // This close_nesting closes either:
    //      jsonhandler->open_nesting(header); or
    //      jsonhandler->open_nesting("UNKNOWN STATS");
    //      From the top (beginning of function).
    if (jsonhandler != NULL){ jsonhandler->close_nesting();}
}
