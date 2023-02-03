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

#include <math.h>
#include <algorithm>
#include <arpa/inet.h>

#include "client.h"
#include "cluster_client.h"


bool client::setup_client(benchmark_config *config, abstract_protocol *protocol, object_generator *objgen)
{
    m_config = config;
    assert(m_config != NULL);
    unsigned long long total_num_of_clients = config->clients*config->threads;

    // create main connection
    shard_connection* conn = new shard_connection(m_connections.size(), this, m_config, m_event_base, protocol);
    m_connections.push_back(conn);

    m_obj_gen = objgen->clone();
    assert(m_obj_gen != NULL);

    if (config->distinct_client_seed && config->randomize)
        m_obj_gen->set_random_seed(config->randomize + config->next_client_idx);
    else if (config->randomize)
        m_obj_gen->set_random_seed(config->randomize);
    else if (config->distinct_client_seed)
        m_obj_gen->set_random_seed(config->next_client_idx);

    // Parallel key-pattern determined according to the first command
    if ((config->arbitrary_commands->is_defined() && config->arbitrary_commands->at(0).key_pattern == 'P') ||
        (config->key_pattern[key_pattern_set]=='P')) {
        unsigned long long client_index = config->next_client_idx % total_num_of_clients;

        unsigned long long range = (config->key_maximum - config->key_minimum)/total_num_of_clients + 1;
        unsigned long long min = config->key_minimum + (range * client_index);
        unsigned long long max = min + range - 1;

        if (client_index == (total_num_of_clients - 1)) {
            max = config->key_maximum; //the last clients takes the leftover
        }

        m_obj_gen->set_key_range(min, max);
    }
    config->next_client_idx++;

    m_keylist = new keylist(m_config->multi_key_get + 1);
    assert(m_keylist != NULL);

    return true;
}

client::client(client_group* group) :
        m_event_base(NULL), m_initialized(false), m_end_set(false), m_config(NULL),
        m_obj_gen(NULL), m_stats(group->get_config()), m_reqs_processed(0), m_reqs_generated(0),
        m_set_ratio_count(0), m_get_ratio_count(0),
        m_arbitrary_command_ratio_count(0), m_executed_command_index(0),
        m_tot_set_ops(0), m_tot_wait_ops(0)
{
    m_event_base = group->get_event_base();

    if (!setup_client(group->get_config(), group->get_protocol(), group->get_obj_gen())) {
        return;
    }

    benchmark_debug_log("new client %p successfully set up.\n", this);
    m_initialized = true;
}

client::client(struct event_base *event_base, benchmark_config *config,
               abstract_protocol *protocol, object_generator *obj_gen) :
        m_event_base(NULL), m_initialized(false), m_end_set(false), m_config(NULL),
        m_obj_gen(NULL), m_stats(config), m_reqs_processed(0), m_reqs_generated(0),
        m_set_ratio_count(0), m_get_ratio_count(0),
        m_arbitrary_command_ratio_count(0), m_executed_command_index(0),
        m_tot_set_ops(0), m_tot_wait_ops(0), m_keylist(NULL)
{
    m_event_base = event_base;

    if (!setup_client(config, protocol, obj_gen)) {
        return;
    }

    benchmark_debug_log("new client %p successfully set up.\n", this);
    m_initialized = true;
}

client::~client()
{
    for (unsigned int i = 0; i < m_connections.size(); i++) {
        shard_connection* sc = m_connections[i];
        delete sc;
    }
    m_connections.clear();

    if (m_obj_gen != NULL) {
        delete m_obj_gen;
        m_obj_gen = NULL;
    }

    if (m_keylist != NULL) {
        delete m_keylist;
        m_keylist = NULL;
    }
}

bool client::initialized(void)
{
    return m_initialized;
}

void client::disconnect(void)
{
    shard_connection* sc = MAIN_CONNECTION;
    assert(sc != NULL);

    sc->disconnect();
}

int client::connect(void)
{
    struct connect_info addr;

    // get primary connection
    shard_connection* sc = MAIN_CONNECTION;
    assert(sc != NULL);

    // get address information
    if (m_config->unix_socket == NULL) {
        if (m_config->server_addr->get_connect_info(&addr) != 0) {
            benchmark_error_log("connect: resolve error: %s\n", m_config->server_addr->get_last_error());
            return -1;
        }

        // Just in case we got domain name and not ip, we convert it
        struct sockaddr_in *ipv4 = (struct sockaddr_in *)addr.ci_addr;
        char address[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(ipv4->sin_addr), address, INET_ADDRSTRLEN);

        char port_str[20];
        snprintf(port_str, sizeof(port_str)-1, "%u", m_config->port);

        // save address and port
        sc->set_address_port(address, port_str);
    }

    // call connect
    int ret = sc->connect(&addr);
    if (ret)
        return ret;

    return 0;
}

bool client::finished(void)
{
    if (m_config->requests > 0 && m_reqs_processed >= m_config->requests)
        return true;
    if (m_config->test_time > 0 && m_stats.get_duration() >= m_config->test_time)
        return true;
    return false;
}

void client::set_start_time() {
    struct timeval now;

    gettimeofday(&now, NULL);
    m_stats.set_start_time(&now);
}

void client::set_end_time() {
    // update only once
    if (!m_end_set) {
        benchmark_debug_log("nothing else to do, test is finished.\n");

        m_stats.set_end_time(NULL);
        m_end_set = true;
    }
}

bool client::hold_pipeline(unsigned int conn_id) {
    // don't exceed requests
    if (m_config->requests) {
        if (m_reqs_generated >= m_config->requests)
            return true;
    }

    // if we have reconnect_interval stop enlarging the pipeline on time
    if (m_config->reconnect_interval) {
        if ((m_reqs_processed % m_config->reconnect_interval) + (m_reqs_generated - m_reqs_processed) >= m_config->reconnect_interval)
            return true;
    }

    return false;
}

void client::create_arbitrary_request(const arbitrary_command* cmd, struct timeval& timestamp, unsigned int conn_id) {
    int cmd_size = 0;

    benchmark_debug_log("%s [%s]:\n", cmd->command_name.c_str(), cmd->command.c_str());

    for (unsigned int i = 0; i < cmd->command_args.size(); i++) {
        const command_arg* arg = &cmd->command_args[i];

        if (arg->type == const_type) {
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg);
        } else if (arg->type == key_type) {
            int iter = get_arbitrary_obj_iter_type(cmd, m_executed_command_index);
            unsigned int key_len;
            const char *key = m_obj_gen->get_key(iter, &key_len);

            assert(key != NULL);
            assert(key_len > 0);

            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg, key, key_len);
        } else if (arg->type == data_type) {
            unsigned int value_len;
            const char *value = m_obj_gen->get_value(0, &value_len);

            assert(value != NULL);
            assert(value_len > 0);

            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg, value, value_len);
        }
    }

    m_connections[conn_id]->send_arbitrary_command_end(m_executed_command_index, &timestamp, cmd_size);
    m_reqs_generated++;
}

// This function could use some urgent TLC -- but we need to do it without altering the behavior
void client::create_request(struct timeval timestamp, unsigned int conn_id)
{
    // are we using arbitrary command?
    if (m_config->arbitrary_commands->is_defined()) {
        const arbitrary_command* executed_command = m_config->arbitrary_commands->get_next_executed_command(m_arbitrary_command_ratio_count,
                                                                                                      m_executed_command_index);
        create_arbitrary_request(executed_command, timestamp, conn_id);

        return;
    }

    // If the Set:Wait ratio is not 0, start off with WAITs
    if (m_config->wait_ratio.b &&
        (m_tot_wait_ops == 0 ||
         (m_tot_set_ops/m_tot_wait_ops > m_config->wait_ratio.a/m_config->wait_ratio.b))) {

        m_tot_wait_ops++;

        unsigned int num_slaves = m_obj_gen->random_range(m_config->num_slaves.min, m_config->num_slaves.max);
        unsigned int timeout = m_obj_gen->normal_distribution(m_config->wait_timeout.min,
                                  m_config->wait_timeout.max, 0,
                                  ((m_config->wait_timeout.max - m_config->wait_timeout.min)/2.0) + m_config->wait_timeout.min);

        m_connections[conn_id]->send_wait_command(&timestamp, num_slaves, timeout);
        m_reqs_generated++;
    }
    // are we set or get? this depends on the ratio
    else if (m_set_ratio_count < m_config->ratio.a) {
        // set command
        data_object *obj = m_obj_gen->get_object(obj_iter_type(m_config, 0));
        unsigned int key_len;
        const char *key = obj->get_key(&key_len);
        unsigned int value_len;
        const char *value = obj->get_value(&value_len);

        m_connections[conn_id]->send_set_command(&timestamp, key, key_len,
                                                 value, value_len, obj->get_expiry(),
                                                 m_config->data_offset);
        m_reqs_generated++;
        m_set_ratio_count++;
        m_tot_set_ops++;
    } else if (m_get_ratio_count < m_config->ratio.b) {
        // get command
        int iter = obj_iter_type(m_config, 2);

        if (m_config->multi_key_get > 0) {
            unsigned int keys_count;

            keys_count = m_config->ratio.b - m_get_ratio_count;
            if ((int)keys_count > m_config->multi_key_get)
                keys_count = m_config->multi_key_get;

            m_keylist->clear();
            while (m_keylist->get_keys_count() < keys_count) {
                unsigned int keylen;
                const char *key = m_obj_gen->get_key(iter, &keylen);

                assert(key != NULL);
                assert(keylen > 0);

                m_keylist->add_key(key, keylen);
            }

            m_connections[conn_id]->send_mget_command(&timestamp, m_keylist);
            m_reqs_generated++;
            m_get_ratio_count += keys_count;
        } else {
            unsigned int keylen;
            const char *key = m_obj_gen->get_key(iter, &keylen);
            assert(key != NULL);
            assert(keylen > 0);

            m_connections[conn_id]->send_get_command(&timestamp, key, keylen, m_config->data_offset);
            m_reqs_generated++;
            m_get_ratio_count++;
        }
    } else {
        // overlap counters
        m_get_ratio_count = m_set_ratio_count = 0;
    }
}

int client::prepare(void)
{
    if (MAIN_CONNECTION == NULL)
        return -1;

    int ret = this->connect();
    if (ret < 0) {
        benchmark_error_log("prepare: failed to connect, test aborted.\n");
        return ret;
    }

    return 0;
}

void client::handle_response(unsigned int conn_id, struct timeval timestamp,
                             request *request, protocol_response *response)
{
    if (response->is_error()) {
        benchmark_error_log("server %s handle error response: %s\n",
                            m_connections[conn_id]->get_readable_id(),
                            response->get_status());
    }

    switch (request->m_type) {
        case rt_get:
            m_stats.update_get_op(&timestamp,
                                  request->m_size + response->get_total_len(),
                                  ts_diff(request->m_sent_time, timestamp),
                                  response->get_hits(),
                                  request->m_keys - response->get_hits());
            break;
        case rt_set:
            m_stats.update_set_op(&timestamp,
                                  request->m_size + response->get_total_len(),
                                  ts_diff(request->m_sent_time, timestamp));
            break;
        case rt_wait:
            m_stats.update_wait_op(&timestamp,
                                   ts_diff(request->m_sent_time, timestamp));
            break;
        case rt_arbitrary: {
            arbitrary_request *ar = static_cast<arbitrary_request *>(request);
            m_stats.update_arbitrary_op(&timestamp,
                                        request->m_size + response->get_total_len(),
                                        ts_diff(request->m_sent_time, timestamp),
                                        ar->index);
            break;
        }
        default:
            assert(0);
            break;
    }
}

///////////////////////////////////////////////////////////////////////////

verify_client::verify_client(struct event_base *event_base,
    benchmark_config *config,
    abstract_protocol *protocol,
    object_generator *obj_gen) : client(event_base, config, protocol, obj_gen),
    m_finished(false), m_verified_keys(0), m_errors(0)
{
    MAIN_CONNECTION->get_protocol()->set_keep_value(true);
}

unsigned long long int verify_client::get_verified_keys(void)
{
    return m_verified_keys;
}

unsigned long long int verify_client::get_errors(void)
{
    return m_errors;
}

void verify_client::create_request(struct timeval timestamp, unsigned int conn_id)
{
    // TODO: Refactor client::create_request so this can be unified.
    if (m_set_ratio_count < m_config->ratio.a) {
        // Prepare a GET request that will be compared against a previous
        // SET request.
        data_object *obj = m_obj_gen->get_object(obj_iter_type(m_config, 0));
        unsigned int key_len;
        const char *key = obj->get_key(&key_len);
        unsigned int value_len;
        const char *value = obj->get_value(&value_len);

        m_connections[conn_id]->send_verify_get_command(&timestamp, key, key_len,
                                                        value, value_len, obj->get_expiry(),
                                                        m_config->data_offset);

        m_set_ratio_count++;
    } else if (m_get_ratio_count < m_config->ratio.b) {
        // We don't really care about GET operations, all we do here is keep
        // the object generator synced.
        int iter = obj_iter_type(m_config, 2);

        if (m_config->multi_key_get > 0) {
            unsigned int keys_count;

            keys_count = m_config->ratio.b - m_get_ratio_count;
            if ((int)keys_count > m_config->multi_key_get)
                keys_count = m_config->multi_key_get;
            m_keylist->clear();
            while (m_keylist->get_keys_count() < keys_count) {
                unsigned int keylen;
                const char *key = m_obj_gen->get_key(iter, &keylen);

                assert(key != NULL);
                assert(keylen > 0);

                m_keylist->add_key(key, keylen);
            }

            m_get_ratio_count += keys_count;
        } else {
            unsigned int keylen;
            m_obj_gen->get_key(iter, &keylen);
            m_get_ratio_count++;
        }

        // We don't really send this request, but need to count it to be in sync.
        m_reqs_processed++;
    } else {
        m_get_ratio_count = m_set_ratio_count = 0;
    }
}

void verify_client::handle_response(unsigned int conn_id, struct timeval timestamp,
                                    request *request, protocol_response *response)
{
    unsigned int rvalue_len;
    const char *rvalue = response->get_value(&rvalue_len);
    verify_request *vr = static_cast<verify_request *>(request);

    assert(vr->m_type == rt_get);
    if (response->is_error()) {
        benchmark_error_log("error: request for key [%.*s] failed: %s\n",
            vr->m_key_len, vr->m_key, response->get_status());
        m_errors++;
    } else {
        if (!rvalue || rvalue_len != vr->m_value_len || memcmp(rvalue, vr->m_value, rvalue_len) != 0) {
            benchmark_error_log("error: key [%.*s]: expected [%.*s], got [%.*s]\n",
                vr->m_key_len, vr->m_key,
                vr->m_value_len, vr->m_value,
                rvalue_len, rvalue);
            m_errors++;
        } else {
            benchmark_debug_log("key: [%.*s] verified successfuly.\n",
                vr->m_key_len, vr->m_key);
            m_verified_keys++;
        }
    }
}

bool verify_client::finished(void)
{
    if (m_finished)
        return true;
    if (m_config->requests > 0 && m_reqs_processed >= m_config->requests)
        return true;
    return false;
}

///////////////////////////////////////////////////////////////////////////

client_group::client_group(benchmark_config* config, abstract_protocol *protocol, object_generator* obj_gen) :
    m_base(NULL), m_config(config), m_protocol(protocol), m_obj_gen(obj_gen)
{
    m_base = event_base_new();
    assert(m_base != NULL);

    assert(protocol != NULL);
    assert(obj_gen != NULL);
}

client_group::~client_group(void)
{
    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        client* c = *i;
        delete c;
    }
    m_clients.clear();

    if (m_base != NULL)
        event_base_free(m_base);
    m_base = NULL;
}

int client_group::create_clients(int num)
{
    for (int i = 0; i < num; i++) {
        client* c;

        if (m_config->cluster_mode)
            c = new cluster_client(this);
        else
            c = new client(this);

        assert(c != NULL);

        if (!c->initialized()) {
            delete c;
            return i;
        }

        m_clients.push_back(c);
    }

    return num;
}

int client_group::prepare(void)
{
   for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        client* c = *i;
        int ret = c->prepare();

        if (ret < 0) {
            return ret;
        }
   }

   return 0;
}

void client_group::run(void)
{
    event_base_dispatch(m_base);
}

unsigned long int client_group::get_total_bytes(void)
{
    unsigned long int total_bytes = 0;
    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        total_bytes += (*i)->get_stats()->get_total_bytes();
    }

    return total_bytes;
}

unsigned long int client_group::get_total_ops(void)
{
    unsigned long int total_ops = 0;
    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        total_ops += (*i)->get_stats()->get_total_ops();
    }

    return total_ops;
}

unsigned long int client_group::get_total_latency(void)
{
    unsigned long int total_latency = 0;
    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        total_latency += (*i)->get_stats()->get_total_latency();
    }

    return total_latency;
}

unsigned long int client_group::get_duration_usec(void)
{
    unsigned long int duration = 0;
    unsigned int thread_counter = 1;
    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++, thread_counter++) {
        float factor = ((float)(thread_counter - 1) / thread_counter);
        duration =  factor * duration +  (float)(*i)->get_stats()->get_duration_usec() / thread_counter ;
    }

    return duration;
}

void client_group::merge_run_stats(run_stats* target)
{
    assert(target != NULL);
    unsigned int iteration_counter = 1;
    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        target->merge(*(*i)->get_stats(), iteration_counter++);
    }
}

void client_group::write_client_stats(const char *prefix)
{
    unsigned int client_id = 0;

    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        char filename[PATH_MAX];

        snprintf(filename, sizeof(filename)-1, "%s-%u.csv", prefix, client_id++);
        if (!(*i)->get_stats()->save_csv(filename, m_config)) {
            fprintf(stderr, "error: %s: failed to write client stats.\n", filename);
        }
    }
}
