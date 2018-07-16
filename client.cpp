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

bool client::setup_client(benchmark_config *config, abstract_protocol *protocol, object_generator *objgen)
{
    m_config = config;
    assert(m_config != NULL);

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

    if (config->key_pattern[0]=='P') {
        unsigned long long range = (config->key_maximum - config->key_minimum)/(config->clients*config->threads) + 1;
        unsigned long long min = config->key_minimum + range*config->next_client_idx;
        unsigned long long max = min+range;
        if(config->next_client_idx==(int)(config->clients*config->threads)-1)
            max = config->key_maximum; //the last clients takes the leftover
        m_obj_gen->set_key_range(min, max);
    }
    config->next_client_idx++;

    m_keylist = new keylist(m_config->multi_key_get + 1);
    assert(m_keylist != NULL);

    return true;
}

client::client(client_group* group) :
        m_event_base(NULL), m_initialized(false), m_end_set(false), m_config(NULL),
        m_obj_gen(NULL), m_reqs_processed(0), m_reqs_generated(0),
        m_set_ratio_count(0), m_get_ratio_count(0),
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
        m_obj_gen(NULL), m_reqs_processed(0), m_reqs_generated(0),
        m_set_ratio_count(0), m_get_ratio_count(0),
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

// This function could use some urgent TLC -- but we need to do it without altering the behavior
void client::create_request(struct timeval timestamp, unsigned int conn_id)
{
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
        benchmark_error_log("error response: %s\n", response->get_status());
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
        if (!(*i)->get_stats()->save_csv(filename, m_config->cluster_mode)) {
            fprintf(stderr, "error: %s: failed to write client stats.\n", filename);
        }
    }        
}
///////////////////////////////////////////////////////////////////////////

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
