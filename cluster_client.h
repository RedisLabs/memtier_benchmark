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

#ifndef MEMTIER_BENCHMARK_CLUSTER_CLIENT_H
#define MEMTIER_BENCHMARK_CLUSTER_CLIENT_H

#include <set>
#include "client.h"

#define MAX_SLOTS 16384

// forward declaration
class shard_connection;

class cluster_client : public client {
protected:
    /*
     * Stores the first slot owned by the indexed connection.
     * Since we connect only to primaries we can have at most 16K distinct connections...
     */
    uint16_t m_conn_to_init_slot[MAX_SLOTS];
    // An index-linked array used to store circular lists of slots, one for each shard returned by the SLOTS command.
    uint16_t m_slot_lists[MAX_SLOTS];

    char m_key_buffer[250];
    int m_key_len;

    virtual int connect(void);
    virtual void disconnect(void);

    shard_connection* create_shard_connection(abstract_protocol* abs_protocol);
    bool connect_shard_connection(shard_connection* sc, char* address, char* port);
    bool get_key_for_conn(unsigned int conn_id, int iter, unsigned long long* key_index);
    void handle_moved(unsigned int conn_id, struct timeval timestamp,
                      request *request, protocol_response *response);
    void handle_ask(unsigned int conn_id, struct timeval timestamp,
                    request *request, protocol_response *response);

public:
    cluster_client(client_group* group);
    virtual ~cluster_client();

    // client manager api's
    virtual void handle_cluster_slots(protocol_response *r);
    virtual void create_request(struct timeval timestamp, unsigned int conn_id);
    virtual bool hold_pipeline(unsigned int conn_id);
    virtual void handle_response(unsigned int conn_id, struct timeval timestamp,
                                 request *request, protocol_response *response);
};


#endif //MEMTIER_BENCHMARK_CLUSTER_CLIENT_H
