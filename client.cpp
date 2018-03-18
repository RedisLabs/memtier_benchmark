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

#include "client.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"

float get_2_meaningful_digits(float val)
{
    float log = floor(log10(val));
    float factor = pow(10, log-1); // to save 2 digits
    float new_val = round( val / factor);
    new_val *= factor;
    return new_val;
}

void client_event_handler(evutil_socket_t sfd, short evtype, void *opaque)
{
    client *c = (client *) opaque;

    assert(c != NULL);
    assert(c->get_sockfd() == sfd);

    c->handle_event(evtype);
}

inline long long int ts_diff(struct timeval a, struct timeval b)
{
    unsigned long long aval = a.tv_sec * 1000000 + a.tv_usec;
    unsigned long long bval = b.tv_sec * 1000000 + b.tv_usec;

    return bval - aval;
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

client::request::request(request_type type, unsigned int size, struct timeval* sent_time, unsigned int keys)
    : m_type(type), m_size(size), m_keys(keys)
{
    if (sent_time != NULL)
        m_sent_time = *sent_time;
    else {
        gettimeofday(&m_sent_time, NULL);
    }
}

bool client::setup_client(benchmark_config *config, abstract_protocol *protocol, object_generator *objgen)
{
    m_config = config;
    assert(m_config != NULL);

    if (m_config->unix_socket) {
        m_unix_sockaddr = (struct sockaddr_un *) malloc(sizeof(struct sockaddr_un));
        assert(m_unix_sockaddr != NULL);

        m_unix_sockaddr->sun_family = AF_UNIX;
        strncpy(m_unix_sockaddr->sun_path, m_config->unix_socket, sizeof(m_unix_sockaddr->sun_path)-1);
        m_unix_sockaddr->sun_path[sizeof(m_unix_sockaddr->sun_path)-1] = '\0';
    }

    m_read_buf = evbuffer_new();
    assert(m_read_buf != NULL);

    m_write_buf = evbuffer_new();
    assert(m_write_buf != NULL);    

    m_protocol = protocol->clone();
    assert(m_protocol != NULL);
    m_protocol->set_buffers(m_read_buf, m_write_buf);

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
    m_sockfd(-1), m_unix_sockaddr(NULL), m_event(NULL), m_event_base(NULL),
    m_read_buf(NULL), m_write_buf(NULL), m_initialized(false), m_connected(false),
    m_authentication(auth_none), m_db_selection(select_none),
    m_config(NULL), m_protocol(NULL), m_obj_gen(NULL),
    m_reqs_processed(0),
    m_set_ratio_count(0),
    m_get_ratio_count(0)    
{
    m_event_base = group->get_event_base();

    if (!setup_client(group->get_config(), group->get_protocol(), group->get_obj_gen())) {
        return;
    }
    
    benchmark_debug_log("new client %p successfully set up.\n", this);
    m_initialized = true;
}

client::client(struct event_base *event_base,
    benchmark_config *config,
    abstract_protocol *protocol,
    object_generator *obj_gen) : 
    m_sockfd(-1), m_unix_sockaddr(NULL), m_event(NULL), m_event_base(NULL),
    m_read_buf(NULL), m_write_buf(NULL), m_initialized(false), m_connected(false),
    m_authentication(auth_none), m_db_selection(select_none),
    m_config(NULL), m_protocol(NULL), m_obj_gen(NULL),
    m_reqs_processed(0),
    m_set_ratio_count(0),
    m_get_ratio_count(0),
    m_tot_set_ops(0),
    m_tot_wait_ops(0)
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
    if (m_event != NULL) {
        event_free(m_event);
        m_event = NULL;
    }
    
    if (m_unix_sockaddr != NULL) {
        free(m_unix_sockaddr);
        m_unix_sockaddr = NULL;
    }

    if (m_read_buf != NULL) {
        evbuffer_free(m_read_buf);
        m_read_buf = NULL;
    }

    if (m_write_buf != NULL) {
        evbuffer_free(m_write_buf);
        m_write_buf = NULL;
    }
    
    if (m_sockfd != -1) {
        close(m_sockfd);
        m_sockfd = -1;
    }

    if (m_protocol != NULL) {
        delete m_protocol;
        m_protocol = NULL;
    }

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
    if (m_sockfd != -1) {
        close(m_sockfd);
        m_sockfd = -1;
    }

    evbuffer_drain(m_read_buf, evbuffer_get_length(m_read_buf));
    evbuffer_drain(m_write_buf, evbuffer_get_length(m_write_buf));

    int ret = event_del(m_event);
    assert(ret == 0);

    m_connected = false;
    m_authentication = auth_none;
    m_db_selection = select_none;
}

int client::connect(void)
{
    struct connect_info addr;

    // clean up existing socket/buffers
    if (m_sockfd != -1)
        close(m_sockfd);
    evbuffer_drain(m_read_buf, evbuffer_get_length(m_read_buf));
    evbuffer_drain(m_write_buf, evbuffer_get_length(m_write_buf));

    if (m_unix_sockaddr != NULL) {
        m_sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (m_sockfd < 0) {
            return -errno;
        }
    } else {
        if (m_config->server_addr->get_connect_info(&addr) != 0) {
            benchmark_error_log("connect: resolve error: %s\n", m_config->server_addr->get_last_error());
            return -1;
        }

        // initialize socket
        m_sockfd = socket(addr.ci_family, addr.ci_socktype, addr.ci_protocol);
        if (m_sockfd < 0) {
            return -errno;
        }

        // configure socket behavior
        struct linger ling = {0, 0};
        int flags = 1;
        int error = setsockopt(m_sockfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
        assert(error == 0);

        error = setsockopt(m_sockfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
        assert(error == 0);

        error = setsockopt(m_sockfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
        assert(error == 0);
    }
    
    // set non-blocking behavior
    int flags = 1;
    if ((flags = fcntl(m_sockfd, F_GETFL, 0)) < 0 ||
        fcntl(m_sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
            benchmark_error_log("connect: failed to set non-blocking flag.\n");
            close(m_sockfd);
            m_sockfd = -1;
            return -1;
    }

    // set up event
    if (!m_event) {
        m_event = event_new(m_event_base,
            m_sockfd, EV_WRITE, client_event_handler, (void *)this);    
        assert(m_event != NULL);
    } else {
        int ret = event_del(m_event);
        assert(ret == 0);

        ret = event_assign(m_event, m_event_base,
            m_sockfd, EV_WRITE, client_event_handler, (void *)this);
        assert(ret == 0);
    }
    
    int ret = event_add(m_event, NULL);
    assert(ret == 0);

    // call connect
    if (::connect(m_sockfd, 
        m_unix_sockaddr ? (struct sockaddr *) m_unix_sockaddr : addr.ci_addr,
        m_unix_sockaddr ? sizeof(struct sockaddr_un) : addr.ci_addrlen) == -1) {
        if (errno == EINPROGRESS || errno == EWOULDBLOCK)            
            return 0;
        benchmark_error_log("connect failed, error = %s\n", strerror(errno));
        return -1;
    }
    
    return 0;
}

void client::handle_event(short evtype)
{
    // connect() returning to us?  normally we expect EV_WRITE, but for UNIX domain
    // sockets we workaround since connect() returned immediately, but we don't want
    // to do any I/O from the client::connect() call...
    if (!m_connected && (evtype == EV_WRITE || m_unix_sockaddr != NULL)) {
        int error = -1;
        socklen_t errsz = sizeof(error);

        if (getsockopt(m_sockfd, SOL_SOCKET, SO_ERROR, (void *) &error, &errsz) == -1) {
            benchmark_error_log("connect: error getting connect response (getsockopt): %s\n", strerror(errno));
            return;
        }

        if (error != 0) {
            benchmark_error_log("connect: connection failed: %s\n", strerror(error));
            return;
        }

        m_connected = true;
        if (!m_reqs_processed) {
            process_first_request();
        } else {
            benchmark_debug_log("reconnection complete, proceeding with test\n");
            fill_pipeline();
        }
    }
   
    assert(m_connected == true);
    if ((evtype & EV_WRITE) == EV_WRITE && evbuffer_get_length(m_write_buf) > 0) {
        if (evbuffer_write(m_write_buf, m_sockfd) < 0) {
            if (errno != EWOULDBLOCK) {
                benchmark_error_log("write error: %s\n", strerror(errno));
                disconnect();

                return;
            }
        }
    }

    if ((evtype & EV_READ) == EV_READ) {
        int ret = 1;
        while (ret > 0) {
            ret = evbuffer_read(m_read_buf, m_sockfd, -1); 
        }

        if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            benchmark_error_log("read error: %s\n", strerror(errno));
            disconnect();

            return;
        }
        if (ret == 0) {
            benchmark_error_log("connection dropped.\n");
            disconnect();

            return;
        }
        
        if (evbuffer_get_length(m_read_buf) > 0) {
            process_response();

            // process_response may have disconnected, in which case
            // we just abort and wait for libevent to call us back sometime
            if (!m_connected) {
                return;
            }
                
        }
    }

    // update event
    short new_evtype = 0;
    if (!finished()) {
        new_evtype = EV_READ;
    }
    if (evbuffer_get_length(m_write_buf) > 0) {
        assert(finished() == false);
        new_evtype |= EV_WRITE;
    }

    if (new_evtype) {
        int ret = event_assign(m_event, m_event_base,
            m_sockfd, new_evtype, client_event_handler, (void *)this);
        assert(ret == 0);

        ret = event_add(m_event, NULL);
        assert(ret == 0);
    } else {
        benchmark_debug_log("nothing else to do, test is finished.\n");
        m_stats.set_end_time(NULL);
    }
}

bool client::finished(void)
{
    if (m_config->requests > 0 && m_reqs_processed >= m_config->requests)
        return true;
    if (m_config->test_time > 0 && m_stats.get_duration() >= m_config->test_time)
        return true;
    return false;    
}

bool client::send_conn_setup_commands(struct timeval timestamp)
{
    bool sent = false;

    if (m_config->authenticate && m_authentication != auth_done) {
        if (m_authentication == auth_none) {
            benchmark_debug_log("sending authentication command.\n");
            m_protocol->authenticate(m_config->authenticate);
            m_pipeline.push(new client::request(rt_auth, 0, &timestamp, 0));
            m_authentication = auth_sent;
            sent = true;
        }
    }
    if (m_config->select_db && m_db_selection != select_done) {
        if (m_db_selection == select_none) {
            benchmark_debug_log("sending db selection command.\n");
            m_protocol->select_db(m_config->select_db);
            m_pipeline.push(new client::request(rt_select_db, 0, &timestamp, 0));
            m_db_selection = select_sent;
            sent = true;
        }
    }

    return sent; 
}

bool client::is_conn_setup_done(void)
{
     if (m_config->authenticate && m_authentication != auth_done)
         return false;
     if (m_config->select_db && m_db_selection != select_done)
         return false;
     return true;
}

/*
 * Utility function to get the object iterator type based on the config
 */
static inline
int obj_iter_type(benchmark_config *cfg, unsigned char index)
{
    if (cfg->key_pattern[index] == 'R')
        return OBJECT_GENERATOR_KEY_RANDOM;
    else if (cfg->key_pattern[index] == 'G')
        return OBJECT_GENERATOR_KEY_GAUSSIAN;
    return OBJECT_GENERATOR_KEY_SET_ITER;
}

// This function could use some urgent TLC -- but we need to do it without altering the behavior
void client::create_request(struct timeval timestamp)
{
    int cmd_size = 0;

    // If the Set:Wait ratio is not 0, start off with WAITs
    if (m_config->wait_ratio.b &&
        (m_tot_wait_ops == 0 ||
         (m_tot_set_ops/m_tot_wait_ops > m_config->wait_ratio.a/m_config->wait_ratio.b))) {

        m_tot_wait_ops++;

        unsigned int num_slaves = m_obj_gen->random_range(m_config->num_slaves.min, m_config->num_slaves.max);
        unsigned int timeout = m_obj_gen->normal_distribution(m_config->wait_timeout.min,
                                  m_config->wait_timeout.max, 0,
                                  ((m_config->wait_timeout.max - m_config->wait_timeout.min)/2.0) + m_config->wait_timeout.min);

        benchmark_debug_log("WAIT num_slaves=%u timeout=%u\n", num_slaves, timeout);
        cmd_size = m_protocol->write_command_wait(num_slaves, timeout);
        m_pipeline.push(new client::request(rt_wait, cmd_size, &timestamp, 0));
    }
    // are we set or get? this depends on the ratio
    else if (m_set_ratio_count < m_config->ratio.a) {
        // set command
        data_object *obj = m_obj_gen->get_object(obj_iter_type(m_config, 0));
        unsigned int key_len;
        const char *key = obj->get_key(&key_len);
        unsigned int value_len;
        const char *value = obj->get_value(&value_len);

        m_set_ratio_count++;
        m_tot_set_ops++;

        benchmark_debug_log("SET key=[%.*s] value_len=%u expiry=%u\n",
            key_len, key, value_len, obj->get_expiry());
        cmd_size = m_protocol->write_command_set(key, key_len, value, value_len,
            obj->get_expiry(), m_config->data_offset);

        m_pipeline.push(new client::request(rt_set, cmd_size, &timestamp, 1));
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

            const char *first_key, *last_key;
            unsigned int first_key_len, last_key_len;
            first_key = m_keylist->get_key(0, &first_key_len);
            last_key = m_keylist->get_key(m_keylist->get_keys_count()-1, &last_key_len);
            
            benchmark_debug_log("MGET %d keys [%.*s] .. [%.*s]\n", 
                m_keylist->get_keys_count(), first_key_len, first_key, last_key_len, last_key);

            cmd_size = m_protocol->write_command_multi_get(m_keylist);
            m_get_ratio_count += keys_count;
            m_pipeline.push(new client::request(rt_get, cmd_size, &timestamp, m_keylist->get_keys_count()));
        } else {
            unsigned int keylen;
            const char *key = m_obj_gen->get_key(iter, &keylen);
            assert(key != NULL);
            assert(keylen > 0);
            
            benchmark_debug_log("GET key=[%.*s]\n", keylen, key);
            cmd_size = m_protocol->write_command_get(key, keylen, m_config->data_offset);

            m_get_ratio_count++;
            m_pipeline.push(new client::request(rt_get, cmd_size, &timestamp, 1));
        }
    } else {
        // overlap counters
        m_get_ratio_count = m_set_ratio_count = 0;
    }        
}

void client::fill_pipeline(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);

    while (!finished() && m_pipeline.size() < m_config->pipeline) {
        if (!is_conn_setup_done()) {
            send_conn_setup_commands(now);
            return;
        }

        // don't exceed requests
        if (m_config->requests > 0 && m_reqs_processed + m_pipeline.size() >= m_config->requests)
            break;

        // if we have reconnect_interval stop enlarging the pipeline
        // on time
        if (m_config->reconnect_interval) {
            if ((m_reqs_processed % m_config->reconnect_interval) + m_pipeline.size() >= m_config->reconnect_interval)
                return;
        }

        create_request(now);
    }
}

int client::prepare(void)
{       
    if (!m_unix_sockaddr && (!m_config->server_addr || !m_protocol))
        return -1;
    
    int ret = this->connect();
    if (ret < 0) {
        benchmark_error_log("prepare: failed to connect, test aborted.\n");
        return ret;
    }

    return 0;
}

void client::process_first_request(void)
{
    struct timeval now;

    gettimeofday(&now, NULL);    
    m_stats.set_start_time(&now);
    fill_pipeline();
}

void client::handle_response(struct timeval timestamp, request *request, protocol_response *response)
{
    switch (request->m_type) {
        case rt_get:
            {
                m_stats.update_get_op(&timestamp,
                    request->m_size + response->get_total_len(),
                    ts_diff(request->m_sent_time, timestamp),
                    response->get_hits(),
                    request->m_keys - response->get_hits());

                unsigned int latencies_size = response->get_latencies_count();
                for (unsigned int i = 0; i < latencies_size; i++) {
                    m_stats.update_get_latency_map(response->get_latency());
                }
            }
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

void client::process_response(void)
{
    int ret;
    bool responses_handled = false;

    struct timeval now;
    gettimeofday(&now, NULL);

    client::request* req = m_pipeline.front();
    
    while ((ret = m_protocol->parse_response(ts_diff_now(req->m_sent_time))) > 0) {
        bool error = false;
        protocol_response *r = m_protocol->get_response();

        req = m_pipeline.front();
        m_pipeline.pop();

        if (req->m_type == rt_auth) {
            if (r->is_error()) {
                benchmark_error_log("error: authentication failed [%s]\n", r->get_status());
                error = true;
            } else {
                m_authentication = auth_done;
                benchmark_debug_log("authentication successful.\n");
            }
        } else if (req->m_type == rt_select_db) {
           if (strcmp(r->get_status(), "+OK") != 0) {
                benchmark_error_log("database selection failed.\n");
                error = true;
            } else {
                benchmark_debug_log("database selection successful.\n");
                m_db_selection = select_done;
            }
        } else {
            benchmark_debug_log("handled response (first line): %s, %d hits, %d misses\n",
                r->get_status(),
                r->get_hits(),
                req->m_keys - r->get_hits());

            if (r->is_error()) {
                benchmark_error_log("error response: %s\n", r->get_status());
            }

            handle_response(now, req, r);
            m_reqs_processed += req->m_keys;
            responses_handled = true;
        }
        delete req;
        if (error) {
            return;
        }
    }

    if (ret == -1) {
        benchmark_error_log("error: response parsing failed.\n");
    }

    if (m_config->reconnect_interval > 0 && responses_handled) {
        if ((m_reqs_processed % m_config->reconnect_interval) == 0) {
            assert(m_pipeline.size() == 0);
            benchmark_debug_log("reconnecting, m_reqs_processed = %u\n", m_reqs_processed);
            disconnect();

            ret = connect();
            assert(ret == 0);

            return;
        }
    }

    fill_pipeline();
}

///////////////////////////////////////////////////////////////////////////

verify_client::verify_request::verify_request(request_type type, 
    unsigned int size, 
    struct timeval* sent_time,
    unsigned int keys,
    const char *key,
    unsigned int key_len,
    const char *value,
    unsigned int value_len) : 
    client::request(type, size, sent_time, keys), 
    m_key(NULL), m_key_len(0),
    m_value(NULL), m_value_len(0)
{
    m_key_len = key_len;
    m_key = (char *)malloc(key_len);
    memcpy(m_key, key, m_key_len);

    m_value_len = value_len;
    m_value = (char *)malloc(value_len);
    memcpy(m_value, value, m_value_len);
}

verify_client::verify_request::~verify_request(void)
{
    if (m_key != NULL) {
        free((void *) m_key);
        m_key = NULL;
    }
    if (m_value != NULL) {
        free((void *) m_value);
        m_value = NULL;
    }
}

verify_client::verify_client(struct event_base *event_base,
    benchmark_config *config,
    abstract_protocol *protocol,
    object_generator *obj_gen) : client(event_base, config, protocol, obj_gen),
    m_finished(false), m_verified_keys(0), m_errors(0)
{
    m_protocol->set_keep_value(true);
}

unsigned long long int verify_client::get_verified_keys(void)
{
    return m_verified_keys;
}

unsigned long long int verify_client::get_errors(void)
{
    return m_errors;
}

void verify_client::create_request(struct timeval timestamp)
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
        unsigned int cmd_size;

        m_set_ratio_count++;
        cmd_size = m_protocol->write_command_get(key, key_len, m_config->data_offset);

        m_pipeline.push(new verify_client::verify_request(rt_get,
            cmd_size, &timestamp, 1, key, key_len, value, value_len));
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

void verify_client::handle_response(struct timeval timestamp, request *request, protocol_response *response)
{
    unsigned int rvalue_len;
    const char* key = NULL;
    const char *rvalue = response->get_value(&rvalue_len, key);
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
    if (key != NULL)
        free((void *)key);
    if (rvalue != NULL)
        free((void *)rvalue);
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

crc_verify_client::verify_request::verify_request(request_type type,
                                              unsigned int size,
                                              struct timeval* sent_time,
                                              unsigned int keys,
                                              keylist keylist_source) :
        client::request(type, size, sent_time, keys),
        m_keylist(keylist_source)
{
}

crc_verify_client::verify_request::~verify_request(void)
{
}

crc_verify_client::crc_verify_client(verify_client_group* group) :
        client(dynamic_cast<client_group*>(group)),
        m_verified_keys(0), m_errors(0)
{
    m_protocol->set_keep_value(true);
}

unsigned long int crc_verify_client::get_verified_keys(void)
{
    return m_verified_keys;
}

unsigned long int crc_verify_client::get_errors(void)
{
    return m_errors;
}

void crc_verify_client::create_request(struct timeval timestamp)
{
    int cmd_size = 0;
    // Prepare a GET request that will be compared against a previous
    // SET request.
    if (m_config->multi_key_get > 0) {
        int iter = obj_iter_type(m_config, 2);
        unsigned int keys_count = m_config->multi_key_get;
        m_keylist->clear();
        while (m_keylist->get_keys_count() < keys_count) {
            unsigned int keylen;
            const char *key = m_obj_gen->get_key(iter, &keylen);
            assert(key != NULL);
            assert(keylen > 0);

            m_keylist->add_key(key, keylen);
        }

        const char *first_key, *last_key;
        unsigned int first_key_len, last_key_len;
        first_key = m_keylist->get_key(0, &first_key_len);
        last_key = m_keylist->get_key(m_keylist->get_keys_count() - 1, &last_key_len);

        benchmark_debug_log("MGET %d keys [%.*s] .. [%.*s]\n",
                m_keylist->get_keys_count(), first_key_len, first_key, last_key_len, last_key);

        cmd_size = m_protocol->write_command_multi_get(m_keylist);
        m_get_ratio_count += keys_count;
        m_pipeline.push(new crc_verify_client::verify_request(rt_get, cmd_size, &timestamp, m_keylist->get_keys_count(), *m_keylist));
    } else {
        int iter = obj_iter_type(m_config, 2);
        unsigned int keylen;
        const char *key = m_obj_gen->get_key(iter, &keylen);
        assert(key != NULL);
        assert(keylen > 0);

        m_keylist->clear();
        m_keylist->add_key(key, keylen);

        benchmark_debug_log("CRC verify: GET key=[%.*s]\n", keylen, key);
        cmd_size = m_protocol->write_command_get(key, keylen, m_config->data_offset);
        m_pipeline.push(new crc_verify_client::verify_request(rt_get, cmd_size, &timestamp, 1, *m_keylist));
    }
}

void crc_verify_client::handle_response(struct timeval timestamp, request *request, protocol_response *response)
{
    unsigned int values_count = response->get_values_count();
    verify_request *vr = static_cast<verify_request *>(request);

    assert(vr->m_type == rt_get);
    m_stats.update_get_op(&timestamp,
                          request->m_size + response->get_total_len(),
                          ts_diff(request->m_sent_time, timestamp),
                          response->get_hits(),
                          request->m_keys - response->get_hits());

    unsigned int latencies_size = response->get_latencies_count();
    for (unsigned int i = 0; i < latencies_size; i++) {
        m_stats.update_get_latency_map(response->get_latency());
    }

    if (response->is_error() || !values_count) {
        unsigned int key_length;
        if (values_count == 1) {
            const char* key = vr->m_keylist.get_key(0, &key_length);
            benchmark_error_log("error: request for key [%.*s] failed: %s\n",
                                key_length, key, response->get_status());
        } else {
            benchmark_error_log("error: request for multiple keys failed: %s\n",
                                response->get_status());
            for (unsigned int i = 0; i < vr->m_keys; i++) {

                const char* key = vr->m_keylist.get_key(i, &key_length);
                benchmark_error_log("key:: [%.*s] \n", key_length, key);
            }
        }
        m_errors++;
    } else {
        unsigned int values_count = response->get_values_count();
        for (unsigned int i = 0; i < values_count; i++) {
            unsigned int rvalue_len;
            const char* key = NULL;
            const char *rvalue = response->get_value(&rvalue_len, key);
            uint32_t crc = crc32::calc_crc32(rvalue, rvalue_len - crc32::size);
            const char *crc_buffer = rvalue + dynamic_cast<crc_object_generator *>(m_obj_gen)->get_actual_value_size();
            if (memcmp(crc_buffer, &crc, crc32::size) == 0) {
                benchmark_debug_log("key verified successfuly.\n");
                m_verified_keys++;
            } else {
                benchmark_error_log("error: key verification failed. Expected hash: %u, present hash: %u.\n",
                                    crc, *(uint32_t *) crc_buffer);
                m_errors++;
            }
            if (key != NULL)
                free((void*)key);
            if (rvalue != NULL)
                free((void*)rvalue);
        }
    }
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
        client* c = new client(this);
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
        if (!(*i)->get_stats()->save_csv(filename)) {
            fprintf(stderr, "error: %s: failed to write client stats.\n", filename);
        }
    }        
}

///////////////////////////////////////////////////////////////////////////

verify_client_group::verify_client_group(benchmark_config *cfg, abstract_protocol *protocol, object_generator* obj_gen) :
        client_group(cfg, protocol, obj_gen)
{
}

int verify_client_group::create_clients(int num)
{
    for (int i = 0; i < num; i++) {
        client* c = new crc_verify_client(this);
        assert(c != NULL);

        if (!c->initialized()) {
            delete c;
            return i;
        }

        m_clients.push_back(c);
    }

    return num;
}

void verify_client_group::merge_run_stats(run_stats* target)
{
    client_group::merge_run_stats(target);

    unsigned long int verified_keys = 0;
    unsigned long int errors = 0;
    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        verified_keys += dynamic_cast<crc_verify_client*>(*i)->get_verified_keys();
        errors += dynamic_cast<crc_verify_client*>(*i)->get_errors();
    }

    target->update_verified_keys(verified_keys);
    target->update_errors(errors);
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
    m_ops(0),
    m_verified_keys(0),
    m_errors(0)
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
    m_verified_keys += other.m_verified_keys;
    m_errors += other.m_errors;
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
    m_cur_stats.m_ops_get += hits+misses;
    m_cur_stats.m_get_hits += hits;
    m_cur_stats.m_get_misses += misses;
 
    m_cur_stats.m_total_get_latency += latency;

    m_totals.m_bytes += bytes;
    m_totals.m_ops+= hits + misses;
    m_totals.m_latency += latency;
}

void run_stats::update_get_latency_map(unsigned int latency)
{
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

void run_stats::update_wait_op(struct timeval *ts, unsigned int latency)
{
    roll_cur_stats(ts);
    m_cur_stats.m_ops_wait++;

    m_cur_stats.m_total_wait_latency += latency;

    m_totals.m_ops++;
    m_totals.m_latency += latency;

    m_wait_latency_map[get_2_meaningful_digits((float)latency/1000)]++;
}

void run_stats::update_verified_keys(unsigned long int keys)
{
    m_totals.m_verified_keys += keys;
}

void run_stats::update_errors(unsigned long int errors)
{
    m_totals.m_errors += errors;
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

unsigned long int run_stats::get_verified_keys()
{
    return m_totals.m_verified_keys;
}

unsigned long int run_stats::get_errors()
{
    return m_totals.m_errors;
}

#define AVERAGE(total, count) \
    ((unsigned int) ((count) > 0 ? (total) / (count) : 0))
#define USEC_FORMAT(value) \
    (value) / 1000000, (value) % 1000000

bool run_stats::save_csv(const char *filename)
{
    FILE *f = fopen(filename, "w");
    if (!f) {
        perror(filename);
        return false;
    }

    fprintf(f, "Per-Second Benchmark Data\n");
    fprintf(f, "Second,SET Requests,SET Average Latency,SET Total Bytes,"
               "GET Requests,GET Average Latency,GET Total Bytes,GET Misses, GET Hits,"
               "WAIT Requests,WAIT Average Latency\n");

    unsigned long int total_get_ops = 0;
    unsigned long int total_set_ops = 0;
    unsigned long int total_wait_ops = 0;

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

    result.m_ops_sec_get = (double) totals.m_ops_get / test_duration_usec * 1000000;
    if (totals.m_ops_get > 0) {
        result.m_latency_get = (double) (totals.m_total_get_latency / totals.m_ops_get) / 1000;
    } else {
        result.m_latency_get = 0;
    }
    result.m_bytes_sec_get = (totals.m_bytes_get / 1024.0) / test_duration_usec * 1000000;
    result.m_hits_sec = (double) totals.m_get_hits / test_duration_usec * 1000000;
    result.m_misses_sec = (double) totals.m_get_misses / test_duration_usec * 1000000;

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
}

void result_print_to_json(json_handler * jsonhandler, const char * type, unsigned long int total_ops, float ops, float hits, float miss, float latency, float kbs)
{
    if (jsonhandler != NULL){ // Added for double verification in case someone accidently send NULL.
        jsonhandler->open_nesting(type);
        jsonhandler->write_obj("Ops","%llu", total_ops);
        jsonhandler->write_obj("Ops/sec","%.2f", ops);
        jsonhandler->write_obj("Hits/sec","%.2f", hits);
        jsonhandler->write_obj("Misses/sec","%.2f", miss);
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

void run_stats::print(FILE *out, bool histogram, const char * header/*=NULL*/,  json_handler * jsonhandler/*=NULL*/)
{
    // Add header if not printed:
    if (header != NULL){
        fprintf(out,"\n\n"
                        "%s\n"
                        "========================================================================\n",
                        header);
        if (jsonhandler != NULL){jsonhandler->open_nesting(header);}
    }
    else{
        if (jsonhandler != NULL){jsonhandler->open_nesting("UNKNOWN STATS");}
    }
    
    // aggregate all one_second_stats; we do this only if we have
    // one_second_stats, otherwise it means we're probably printing previously
    // aggregated data
    if (m_stats.size() > 0) {
        summarize(m_totals);
    }

    // print results
    fprintf(out,
           "%-6s %12s %12s %12s %12s %12s\n"
           "------------------------------------------------------------------------\n",
           "Type", "Ops/sec", "Hits/sec", "Misses/sec", "Latency", "KB/sec");
    fprintf(out,
           "%-6s %12.2f %12s %12s %12.05f %12.2f\n",
           "Sets",
           m_totals.m_ops_sec_set,
           "---", "---",
           m_totals.m_latency_set,
           m_totals.m_bytes_sec_set);

    fprintf(out,
           "%-6s %12.2f %12.2f %12.2f %12.05f %12.2f\n",
           "Gets",
           m_totals.m_ops_sec_get,
           m_totals.m_hits_sec,
           m_totals.m_misses_sec,
           m_totals.m_latency_get,
           m_totals.m_bytes_sec_get);

    fprintf(out,
            "%-6s %12.2f %12s %12s %12.05f %12s\n",
            "Waits",
            m_totals.m_ops_sec_wait,
            "---", "---",
            m_totals.m_latency_wait,
            "---");

    fprintf(out,
           "%-6s %12.2f %12.2f %12.2f %12.05f %12.2f\n",
           "Totals",
           m_totals.m_ops_sec,
           m_totals.m_hits_sec,
           m_totals.m_misses_sec,
           m_totals.m_latency,
           m_totals.m_bytes_sec);

    ////////////////////////////////////////
    // JSON print handling
    // ------------------
    if (jsonhandler != NULL){
        result_print_to_json(jsonhandler,"Sets",m_totals.m_ops_set,
                                                m_totals.m_ops_sec_set,
                                                0.0,
                                                0.0,
                                                m_totals.m_latency_set,
                                                m_totals.m_bytes_sec_set);
        result_print_to_json(jsonhandler,"Gets",m_totals.m_ops_get,
                                                m_totals.m_ops_sec_get,
                                                m_totals.m_hits_sec,
                                                m_totals.m_misses_sec,
                                                m_totals.m_latency_get,
                                                m_totals.m_bytes_sec_get);
        result_print_to_json(jsonhandler,"Waits",m_totals.m_ops_wait,
                                                m_totals.m_ops_sec_wait,
                                                0.0,
                                                0.0,
                                                m_totals.m_latency_wait,
                                                0.0);
        result_print_to_json(jsonhandler,"Totals", m_totals.m_ops,
                                                m_totals.m_ops_sec,
                                                m_totals.m_hits_sec,
                                                m_totals.m_misses_sec,
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

