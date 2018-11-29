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

#include <stdio.h>

#include "run_stats_types.h"


void one_sec_cmd_stats::reset() {
    m_bytes = 0;
    m_ops = 0;
    m_hits = 0;
    m_misses = 0;
    m_moved = 0;
    m_ask = 0;
    m_total_latency = 0;
}

void one_sec_cmd_stats::merge(const one_sec_cmd_stats& other) {
    m_bytes += other.m_bytes;
    m_ops += other.m_ops;
    m_hits += other.m_hits;
    m_misses += other.m_misses;
    m_moved += other.m_moved;
    m_ask += other.m_ask;
    m_total_latency += other.m_total_latency;
}

void one_sec_cmd_stats::update_op(unsigned int bytes, unsigned int latency) {
    m_bytes += bytes;
    m_ops++;
    m_total_latency += latency;
}

void one_sec_cmd_stats::update_op(unsigned int bytes, unsigned int latency,
                                  unsigned int hits, unsigned int misses) {
    update_op(bytes, latency);
    m_hits += hits;
    m_misses += misses;
}

void one_sec_cmd_stats::update_moved_op(unsigned int bytes, unsigned int latency) {
    update_op(bytes, latency);
    m_moved++;
}

void one_sec_cmd_stats::update_ask_op(unsigned int bytes, unsigned int latency) {
    update_op(bytes, latency);
    m_ask++;
}

///////////////////////////////////////////////////////////////////////////

one_second_stats::one_second_stats(unsigned int second) {
    reset(second);
}

void one_second_stats::reset(unsigned int second) {
    m_second = second;
    m_get_cmd.reset();
    m_set_cmd.reset();
    m_wait_cmd.reset();
}

void one_second_stats::merge(const one_second_stats& other) {
    m_get_cmd.merge(other.m_get_cmd);
    m_set_cmd.merge(other.m_set_cmd);
    m_wait_cmd.merge(other.m_wait_cmd);
}

///////////////////////////////////////////////////////////////////////////

totals_cmd::totals_cmd() :
        m_ops_sec(0),
        m_bytes_sec(0),
        m_moved_sec(0),
        m_ask_sec(0),
        m_latency(0),
        m_ops(0) {
}

void totals_cmd::add(const totals_cmd& other) {
    m_ops_sec += other.m_ops_sec;
    m_moved_sec += other.m_moved_sec;
    m_ask_sec += other.m_ask_sec;
    m_bytes_sec += other.m_bytes_sec;
    m_latency += other.m_latency;
    m_ops += other.m_ops;
}

void totals_cmd::aggregate_average(size_t stats_size) {
    m_ops_sec /= stats_size;
    m_moved_sec /= stats_size;
    m_ask_sec /= stats_size;
    m_bytes_sec /= stats_size;
    m_latency /= stats_size;
}

void totals_cmd::summarize(const one_sec_cmd_stats& other, unsigned long test_duration_usec) {
    m_ops = other.m_ops;

    m_ops_sec = (double) other.m_ops / test_duration_usec * 1000000;
    if (other.m_ops > 0) {
        m_latency = (double) (other.m_total_latency / other.m_ops) / 1000;
    } else {
        m_latency = 0;
    }

    m_bytes_sec = (other.m_bytes / 1024.0) / test_duration_usec * 1000000;
    m_moved_sec = (double) other.m_moved / test_duration_usec * 1000000;
    m_ask_sec = (double) other.m_ask / test_duration_usec * 1000000;
}

///////////////////////////////////////////////////////////////////////////

totals::totals() :
        m_set_cmd(),
        m_get_cmd(),
        m_wait_cmd(),
        m_ops_sec(0),
        m_bytes_sec(0),
        m_hits_sec(0),
        m_misses_sec(0),
        m_moved_sec(0),
        m_ask_sec(0),
        m_latency(0),
        m_bytes(0),
        m_ops(0) {
}

void totals::add(const totals& other) {
    m_set_cmd.add(other.m_set_cmd);
    m_get_cmd.add(other.m_get_cmd);
    m_wait_cmd.add(other.m_wait_cmd);
    m_ops_sec += other.m_ops_sec;
    m_hits_sec += other.m_hits_sec;
    m_misses_sec += other.m_misses_sec;
    m_moved_sec += other.m_moved_sec;
    m_ask_sec += other.m_ask_sec;
    m_bytes_sec += other.m_bytes_sec;
    m_latency += other.m_latency;
    m_bytes += other.m_bytes;
    m_ops += other.m_ops;
}
