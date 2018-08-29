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

#ifndef MEMTIER_BENCHMARK_CLIENT_DATA_MANAGER_H
#define MEMTIER_BENCHMARK_CLIENT_DATA_MANAGER_H

class connections_manager {
public:
    virtual unsigned long long get_reqs_processed(void) = 0;
    virtual void inc_reqs_processed(void) = 0;
    virtual unsigned long long get_reqs_generated(void) = 0;
    virtual void inc_reqs_generated(void) = 0;
    virtual bool finished(void) = 0;

    virtual void set_start_time(void) = 0;
    virtual void set_end_time(void) = 0;

    virtual void handle_cluster_slots(protocol_response *r) = 0;
    virtual void handle_response(unsigned int conn_id, struct timeval timestamp,
                                 request *request, protocol_response *response) = 0;

    virtual void create_request(struct timeval timestamp, unsigned int conn_id) = 0;
    virtual bool hold_pipeline(unsigned int conn_id) = 0;

    virtual int connect(void) = 0;
    virtual void disconnect(void) = 0;
};


#endif //MEMTIER_BENCHMARK_CLIENT_DATA_MANAGER_H
