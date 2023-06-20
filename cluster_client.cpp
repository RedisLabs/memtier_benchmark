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

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "cluster_client.h"
#include "memtier_benchmark.h"
#include "obj_gen.h"
#include "shard_connection.h"

#define KEY_INDEX_QUEUE_MAX_SIZE 1000000

#define MOVED_MSG_PREFIX "-MOVED"
#define MOVED_MSG_PREFIX_LEN 6
#define ASK_MSG_PREFIX "-ASK"
#define ASK_MSG_PREFIX_LEN 4

#define MAX_CLUSTER_HSLOT 16383
static const uint16_t crc16tab[256]= {
        0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
        0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
        0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
        0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
        0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
        0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
        0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
        0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
        0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
        0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
        0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
        0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
        0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
        0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
        0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
        0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
        0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
        0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
        0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
        0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
        0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
        0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
        0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
        0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
        0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
        0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
        0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
        0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
        0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
        0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
        0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
        0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

static inline uint16_t crc16(const char *buf, size_t len) {
    size_t counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
    return crc;
}

static uint32_t calc_hslot_crc16_cluster(const char *str, size_t length)
{
    uint32_t rv = (uint32_t) crc16(str, length) & MAX_CLUSTER_HSLOT;
    return rv;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

cluster_client::cluster_client(client_group* group) : client(group)
{
}

cluster_client::~cluster_client() {
    for (unsigned int i = 0; i < m_key_index_pools.size(); i++) {
        key_index_pool* key_idx_pool = m_key_index_pools[i];
        delete key_idx_pool;
    }
    m_key_index_pools.clear();
}

int cluster_client::connect(void) {
    // get main connection
    shard_connection* sc = MAIN_CONNECTION;
    assert(sc != NULL);

    // set main connection to send 'CLUSTER SLOTS' command
    sc->set_cluster_slots();

    // create key index pool for main connection
    key_index_pool* key_idx_pool = new key_index_pool;
    m_key_index_pools.push_back(key_idx_pool);
    assert(m_connections.size() == m_key_index_pools.size());

    // continue with base class
    client::connect();

    return 0;
}

void cluster_client::disconnect(void)
{
    unsigned int conn_size = m_connections.size();
    unsigned int i;

    // disconnect all connections
    for (i = 0; i < m_connections.size(); i++) {
        shard_connection* sc = m_connections[i];
        sc->disconnect();
    }

    // delete all connections except main connection
    for (i = conn_size - 1; i > 0; i--) {
        shard_connection* sc = m_connections.back();
        m_connections.pop_back();
        delete sc;
    }
}

shard_connection* cluster_client::create_shard_connection(abstract_protocol* abs_protocol) {
    shard_connection* sc = new shard_connection(m_connections.size(), this,
                                                m_config, m_event_base,
                                                abs_protocol);
    assert(sc != NULL);

    m_connections.push_back(sc);

    // create key index pool
    key_index_pool* key_idx_pool = new key_index_pool;
    assert(key_idx_pool != NULL);

    m_key_index_pools.push_back(key_idx_pool);
    assert(m_connections.size() == m_key_index_pools.size());

    return sc;
}

bool cluster_client::connect_shard_connection(shard_connection* sc, char* address, char* port) {
    // empty key index queue
    if (m_key_index_pools[sc->get_id()]->size()) {
        key_index_pool empty_queue;
        std::swap(*m_key_index_pools[sc->get_id()], empty_queue);
    }

    // save address and port
    sc->set_address_port(address, port);

    // get address information
    struct connect_info ci;
    struct addrinfo *addr_info;
    struct addrinfo hints;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;

    int res = getaddrinfo(address, port, &hints, &addr_info);
    if (res != 0) {
        benchmark_error_log("connect: resolve error: %s\n", gai_strerror(res));
        return false;
    }

    ci.ci_family = addr_info->ai_family;
    ci.ci_socktype = addr_info->ai_socktype;
    ci.ci_protocol = addr_info->ai_protocol;
    assert(addr_info->ai_addrlen <= sizeof(ci.addr_buf));
    memcpy(ci.addr_buf, addr_info->ai_addr, addr_info->ai_addrlen);
    ci.ci_addr = (struct sockaddr *) ci.addr_buf;
    ci.ci_addrlen = addr_info->ai_addrlen;
    freeaddrinfo(addr_info);

    // call connect
    res = sc->connect(&ci);

    return res == 0;
}

void cluster_client::handle_cluster_slots(protocol_response *r) {
    /*
     * temporary array to test if some of the connections are left with no
     * slots, and need to be closed.
     */
    unsigned long prev_connections_size = m_connections.size();
    std::vector<bool> close_sc(prev_connections_size, true);

    // run over response and create connections
    for (unsigned int i=0; i<r->get_mbulk_value()->mbulks_elements.size(); i++) {
        // create connection
        mbulk_size_el* shard = r->get_mbulk_value()->mbulks_elements[i]->as_mbulk_size();

        int min_slot = strtol(shard->mbulks_elements[0]->as_bulk()->value + 1, NULL, 10);
        int max_slot = strtol(shard->mbulks_elements[1]->as_bulk()->value + 1, NULL, 10);

        // hostname/ip
        bulk_el* mbulk_addr_el = shard->mbulks_elements[2]->as_mbulk_size()->mbulks_elements[0]->as_bulk();
        char* addr = (char*) malloc(mbulk_addr_el->value_len + 1);
        memcpy(addr, mbulk_addr_el->value, mbulk_addr_el->value_len);
        addr[mbulk_addr_el->value_len] = '\0';

        // port
        bulk_el* mbulk_port_el = shard->mbulks_elements[2]->as_mbulk_size()->mbulks_elements[1]->as_bulk();
        char* port = (char*) malloc(mbulk_port_el->value_len + 1);
        memcpy(port, mbulk_port_el->value + 1, mbulk_port_el->value_len);
        port[mbulk_port_el->value_len] = '\0';

        // check if connection already exist
        shard_connection* sc = NULL;
        unsigned int j;

        for (j = 0; j < m_connections.size(); j++) {
            if (strcmp(addr, m_connections[j]->get_address()) == 0 &&
                strcmp(port, m_connections[j]->get_port()) == 0) {
                sc = m_connections[j];

                // mark not to close this connection
                if (j < prev_connections_size)
                    close_sc[j] = false;

                // if connection disconnected, try to reconnect
                if (sc->get_connection_state() == conn_disconnected) {
                    connect_shard_connection(sc, addr, port);
                }

                break;
            }
        }

        // if connection doesn't exist, add it
        if (sc == NULL) {
            sc = create_shard_connection(MAIN_CONNECTION->get_protocol());
            connect_shard_connection(sc, addr, port);
        }

        // update range
        for (int j = min_slot; j <= max_slot; j++) {
            m_slot_to_shard[j] = sc->get_id();
        }

        free(addr);
        free(port);
    }

    // check if some connections left with no slots, and need to be closed
    for (unsigned int i=0; i < prev_connections_size; i++) {
        if ((close_sc[i] == true) &&
            (m_connections[i]->get_connection_state() != conn_disconnected)) {

            m_connections[i]->disconnect();
        }
    }
}

bool cluster_client::hold_pipeline(unsigned int conn_id) {
    if (m_connections[conn_id]->get_connection_state() == conn_disconnected) {
        return true;
    }

    /* Don't exceed requests. */
    if (m_config->requests) {
        if (m_key_index_pools[conn_id]->empty() &&
            m_reqs_generated >= m_config->requests) {
            return true;
        }
    }

    return false;
}

get_key_response cluster_client::get_key_for_conn(unsigned int command_index, unsigned int conn_id, unsigned long long* key_index) {
    // first check if we already have a key in the pool
    if (!m_key_index_pools[conn_id]->empty()) {
        *key_index = m_key_index_pools[conn_id]->front();
        m_key_len = snprintf(m_key_buffer, sizeof(m_key_buffer)-1, "%s%llu", m_obj_gen->get_key_prefix(), *key_index);

        m_key_index_pools[conn_id]->pop();
        return available_for_conn;
    }

    // generate key
    client::get_key_for_conn(command_index, conn_id, key_index);

    unsigned int hslot = calc_hslot_crc16_cluster(m_key_buffer, m_key_len);

    // check if the key match for this connection
    if (m_slot_to_shard[hslot] == conn_id) {
        benchmark_debug_log("%s generated key=[%.*s] for itself\n", m_connections[conn_id]->get_readable_id(), m_key_len, m_key_buffer);
        return available_for_conn;
    }

    // handle key for other connection
    unsigned int other_conn_id = m_slot_to_shard[hslot];

    // in case we generated key for connection that is disconnected, 'slot to shard' map may need to be updated
    if (m_connections[other_conn_id]->get_connection_state() == conn_disconnected) {
        m_connections[conn_id]->set_cluster_slots();
        return not_available;
    }

    // in case connection is during cluster slots command, his slots mapping not relevant
    if (m_connections[other_conn_id]->get_cluster_slots_state() != setup_done)
        return not_available;

    key_index_pool* key_idx_pool = m_key_index_pools[other_conn_id];
    if (key_idx_pool->size() >= KEY_INDEX_QUEUE_MAX_SIZE)
        return not_available;

    // store command and key for the other connection
    benchmark_debug_log("%s generated key=[%.*s] for %s\n", m_connections[conn_id]->get_readable_id(), m_key_len, m_key_buffer, m_connections[other_conn_id]->get_readable_id());

    key_idx_pool->push(command_index);
    key_idx_pool->push(*key_index);
    return available_for_other_conn;
}

bool cluster_client::create_arbitrary_request(unsigned int command_index, struct timeval& timestamp, unsigned int conn_id) {
    /* In arbitrary request, where we send the command arg by arg, we need to check for a key command,
     * if the generated key belongs to this connection before starting to send it */
    assert(m_key_index_pools[conn_id]->empty());

    /* keyless command can be used by any connection */
    if (get_arbitrary_command(command_index).keys_count == 0) {
        client::create_arbitrary_request(command_index, timestamp, conn_id);
        return true;
    }

    unsigned long long key_index;
    get_key_response res = get_key_for_conn(command_index, conn_id, &key_index);

    if (res == not_available)
        return false;

    /* If we generated a key for a different connection, we will use it later */
    if (res == available_for_other_conn)
        return true;

    /* We got a key for this connection, put it back into the pool and
     * use it inside client::create_arbitrary_request() */
    m_key_index_pools[conn_id]->push(key_index);
    client::create_arbitrary_request(command_index, timestamp, conn_id);

    return true;
}

void cluster_client::create_request(struct timeval timestamp, unsigned int conn_id) {
    /* If pool is empty continue with base class */
    if (m_key_index_pools[conn_id]->empty()) {
        client::create_request(timestamp, conn_id);
        return;
    }

    unsigned int pool_size = m_key_index_pools[conn_id]->size();
    unsigned int command_index = m_key_index_pools[conn_id]->front();
    m_key_index_pools[conn_id]->pop();

    if (m_config->arbitrary_commands->is_defined())
        client::create_arbitrary_request(command_index, timestamp, conn_id);
    else if (command_index == SET_CMD_IDX)
        create_set_request(timestamp, conn_id);
    else if (command_index == GET_CMD_IDX)
        create_get_request(timestamp, conn_id);
    else
        assert("Unexpected command index");

    /* Make sure we used pair of command and key index */
    assert(m_key_index_pools[conn_id]->size() == pool_size - 2);
}

// In case of -MOVED response, we sends CLUSTER SLOTS command to get the new topology
void cluster_client::handle_moved(unsigned int conn_id, struct timeval timestamp,
                                  request *request, protocol_response *response) {
    // update stats
    if (request->m_type == rt_get) {
        m_stats.update_moved_get_op(&timestamp,
                                    request->m_size + response->get_total_len(),
                                    ts_diff(request->m_sent_time, timestamp));
    } else if (request->m_type == rt_set) {
        m_stats.update_moved_set_op(&timestamp,
                                    request->m_size + response->get_total_len(),
                                    ts_diff(request->m_sent_time, timestamp));
     } else if (request->m_type == rt_arbitrary) {
        arbitrary_request *ar = static_cast<arbitrary_request *>(request);
        m_stats.update_moved_arbitrary_op(&timestamp,
                                    request->m_size + response->get_total_len(),
                                    ts_diff(request->m_sent_time, timestamp),
                                    ar->index);
    } else {
        assert(0);
    }

    // connection already issued 'cluster slots' command, wait for slots mapping to be updated
    if (m_connections[conn_id]->get_cluster_slots_state() != setup_done)
        return;

    // queue may stored uncorrected mapping indexes, empty them
    key_index_pool empty_queue;
    std::swap(*m_key_index_pools[conn_id], empty_queue);

    // set connection to send 'CLUSTER SLOTS' command
    m_connections[conn_id]->set_cluster_slots();
}

// In case of -ASK response, we ignore the response and we will update to the new topology when we get -MOVED response
void cluster_client::handle_ask(unsigned int conn_id, struct timeval timestamp,
                                request *request, protocol_response *response) {
    // update stats
    if (request->m_type == rt_get) {
        m_stats.update_ask_get_op(&timestamp,
                                    request->m_size + response->get_total_len(),
                                    ts_diff(request->m_sent_time, timestamp));
    } else if (request->m_type == rt_set) {
        m_stats.update_ask_set_op(&timestamp,
                                    request->m_size + response->get_total_len(),
                                    ts_diff(request->m_sent_time, timestamp));
    } else if (request->m_type == rt_arbitrary) {
        arbitrary_request *ar = static_cast<arbitrary_request *>(request);
        m_stats.update_ask_arbitrary_op(&timestamp,
                                    request->m_size + response->get_total_len(),
                                    ts_diff(request->m_sent_time, timestamp),
                                    ar->index);
    } else {
        assert(0);
    }
}

void cluster_client::handle_response(unsigned int conn_id, struct timeval timestamp,
                                     request *request, protocol_response *response) {
    if (response->is_error()) {
        benchmark_debug_log("server %s handle response: %s\n",
                            m_connections[conn_id]->get_readable_id(),
                            response->get_status());
        // handle "-MOVED"
        if (strncmp(response->get_status(), MOVED_MSG_PREFIX, MOVED_MSG_PREFIX_LEN) == 0) {
            handle_moved(conn_id, timestamp, request, response);
            return;
        }

        // handle "-ASK"
        if (strncmp(response->get_status(), ASK_MSG_PREFIX, ASK_MSG_PREFIX_LEN) == 0) {
            handle_ask(conn_id, timestamp, request, response);
            return;
        }
    }

    // continue with base class
    client::handle_response(conn_id, timestamp, request, response);
}

