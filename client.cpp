/*
 * Copyright (C) 2011-2013 Garantia Data Ltd.
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

#include <algorithm>

#include "client.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"

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
    } else {
        struct addrinfo hints;    
        char port_str[20];

        memset(&hints, 0, sizeof(hints));
        hints.ai_flags = AI_PASSIVE;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_family = AF_INET;      // Don't play with IPv6 for now...

        snprintf(port_str, sizeof(port_str)-1, "%u", m_config->port);
        int error = getaddrinfo(m_config->server, 
                        port_str, &hints, &m_server_addr);
        if (error != 0) {
            benchmark_error_log("%s:%u: failed to resolve: %s\n", 
                        m_config->server, m_config->port, gai_strerror(error));
            return false;
        }
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

    m_keylist = new keylist(m_config->multi_key_get + 1);
    assert(m_keylist != NULL);

    return true;
}

client::client(client_group* group) : 
    m_sockfd(-1), m_server_addr(NULL), m_unix_sockaddr(NULL), m_event(NULL), m_event_base(NULL), 
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
    m_sockfd(-1), m_server_addr(NULL), m_unix_sockaddr(NULL), m_event(NULL), m_event_base(NULL), 
    m_read_buf(NULL), m_write_buf(NULL), m_initialized(false), m_connected(false),
    m_authentication(auth_none), m_db_selection(select_none),
    m_config(NULL), m_protocol(NULL), m_obj_gen(NULL),
    m_reqs_processed(0),
    m_set_ratio_count(0),
    m_get_ratio_count(0)    
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
    
    if (m_server_addr != NULL) {
        freeaddrinfo(m_server_addr);
        m_server_addr = NULL;
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
    if (!m_server_addr && !m_unix_sockaddr) {
        benchmark_error_log("connect: server host/port failed to resolve, aborting.\n");
        return -1;
    }

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
        // initialize socket
        m_sockfd = socket(m_server_addr->ai_family, m_server_addr->ai_socktype, m_server_addr->ai_protocol);
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
    
    // set non-blcoking behavior
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
        m_server_addr != NULL ? m_server_addr->ai_addr : (struct sockaddr *) m_unix_sockaddr, 
        m_server_addr != NULL ? m_server_addr->ai_addrlen : sizeof(struct sockaddr_un)) == -1) {
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

bool client::send_conn_setup_commands(void)
{
    bool sent = false;

    if (m_config->authenticate && m_authentication != auth_done) {
        if (m_authentication == auth_none) {
            benchmark_debug_log("sending authentication command.\n");
            m_protocol->authenticate(m_config->authenticate);
            m_pipeline.push(new client::request(rt_auth, 0, NULL, 0));
            m_authentication = auth_sent;
            sent = true;
        }
    }
    if (m_config->select_db && m_db_selection != select_done) {
        if (m_db_selection == select_none) {
            benchmark_debug_log("sending db selection command.\n");
            m_protocol->select_db(m_config->select_db);
            m_pipeline.push(new client::request(rt_select_db, 0, NULL, 0));
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

void client::create_request(void)
{
    // are we set or get? this depends on the ratio
    if (m_set_ratio_count < m_config->ratio.a) {
        // set command
    
        data_object *obj = m_obj_gen->get_object(m_config->key_pattern[0] == 'R' ? 0 : 1);
        unsigned int key_len;
        const char *key = obj->get_key(&key_len);
        unsigned int value_len;
        const char *value = obj->get_value(&value_len);
        int cmd_size = 0;

        m_set_ratio_count++;
        benchmark_debug_log("SET key=[%.*s] value_len=%u expiry=%u\n",
            key_len, key, value_len, obj->get_expiry());
        cmd_size = m_protocol->write_command_set(key, key_len, value, value_len,
            obj->get_expiry());

        m_pipeline.push(new client::request(rt_set, cmd_size, NULL, 1));
    } else if (m_get_ratio_count < m_config->ratio.b) {
        // get command
        int cmd_size = 0;
        
        if (m_config->multi_key_get > 0) {
            unsigned int keys_count;                

            keys_count = m_config->ratio.b - m_get_ratio_count;
            if ((int)keys_count > m_config->multi_key_get)
                keys_count = m_config->multi_key_get;

            m_keylist->clear();
            while (m_keylist->get_keys_count() < keys_count) {
                unsigned int keylen;
                const char *key = m_obj_gen->get_key(m_config->key_pattern[2] == 'R' ? 0 : 2, &keylen);

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
            m_pipeline.push(new client::request(rt_get, cmd_size, NULL, m_keylist->get_keys_count()));
        } else {
            unsigned int keylen;
            const char *key = m_obj_gen->get_key(m_config->key_pattern[2] == 'R' ? 0 : 2, &keylen);
            assert(key != NULL);
            assert(keylen > 0);
            
            benchmark_debug_log("GET key=[%.*s]\n", keylen, key);
            cmd_size = m_protocol->write_command_get(key, keylen);

            m_get_ratio_count++;
            m_pipeline.push(new client::request(rt_get, cmd_size, NULL, 1));
        }
    } else {
        // overlap counters
        m_get_ratio_count = m_set_ratio_count = 0;
    }        
}

void client::fill_pipeline(void)
{
   
    while (!finished() && m_pipeline.size() < m_config->pipeline) {
        if (!is_conn_setup_done()) {
            send_conn_setup_commands();
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

        create_request();
    }
}

int client::prepare(void)
{       
    if (!m_unix_sockaddr && (!m_server_addr || !m_protocol))
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

void client::handle_response(request *request, protocol_response *response)
{
    switch (request->m_type) {
        case rt_get:
            m_stats.update_get_op(NULL, 
                request->m_size + response->get_total_len(),
                ts_diff_now(request->m_sent_time),
                response->get_hits(),
                request->m_keys - response->get_hits());
            break;
        case rt_set:
            m_stats.update_set_op(NULL,
                request->m_size + response->get_total_len(),
                ts_diff_now(request->m_sent_time));
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
    
    while ((ret = m_protocol->parse_response()) > 0) {
        bool error = false;
        protocol_response *r = m_protocol->get_response();

        client::request* req = m_pipeline.front();
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

            handle_response(req, r);
                
            m_reqs_processed++;
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

void verify_client::create_request(void)
{
    // TODO: Refactor client::create_reqeust so this can be unified.
    if (m_set_ratio_count < m_config->ratio.a) {
        // Prepare a GET request that will be compared against a previous
        // SET request.
        data_object *obj = m_obj_gen->get_object(m_config->key_pattern[0] == 'R' ? 0 : 1);
        unsigned int key_len;
        const char *key = obj->get_key(&key_len);
        unsigned int value_len;
        const char *value = obj->get_value(&value_len);
        unsigned int cmd_size;

        m_set_ratio_count++;
        cmd_size = m_protocol->write_command_get(key, key_len);

        m_pipeline.push(new verify_client::verify_request(rt_get,
            cmd_size, NULL, 1, key, key_len, value, value_len));
    } else if (m_get_ratio_count < m_config->ratio.b) {
        // We don't really care about GET operations, all we do here is keep
        // the object generator synced.
        if (m_config->multi_key_get > 0) {
            unsigned int keys_count;

            keys_count = m_config->ratio.b - m_get_ratio_count;
            if ((int)keys_count > m_config->multi_key_get)
                keys_count = m_config->multi_key_get;
            m_keylist->clear();
            while (m_keylist->get_keys_count() < keys_count) {
                unsigned int keylen;
                const char *key = m_obj_gen->get_key(m_config->key_pattern[2] == 'R' ? 0 : 2, &keylen);

                assert(key != NULL);
                assert(keylen > 0);

                m_keylist->add_key(key, keylen);
            }

            m_get_ratio_count += keys_count;
        } else {
            unsigned int keylen;
            m_obj_gen->get_key(m_config->key_pattern[2] == 'R' ? 0 : 2, &keylen);
            m_get_ratio_count++;
        }

        // We don't really send this request, but need to count it to be in sync.
        m_reqs_processed++;
    } else {
        m_get_ratio_count = m_set_ratio_count = 0;
    }
}

void verify_client::handle_response(request *request, protocol_response *response)
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
    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        if ((*i)->get_stats()->get_duration_usec() > duration)
            duration = (*i)->get_stats()->get_duration_usec();
    }
        
    return duration;
}

void client_group::merge_run_stats(run_stats* target)
{
    assert(target != NULL);

    for (std::vector<client*>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        target->merge(*(*i)->get_stats());
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

run_stats::one_second_stats::one_second_stats(unsigned int second)
{
    reset(second);
}

void run_stats::one_second_stats::reset(unsigned int second)
{
    m_second = second;
    m_bytes_get = m_bytes_set = 0;
    m_ops_get = m_ops_set = 0;
    m_get_hits = m_get_misses = 0;
    m_total_get_latency = 0;
    m_total_set_latency = 0;
}

void run_stats::one_second_stats::merge(const one_second_stats& other)
{
    m_bytes_get += other.m_bytes_get;
    m_bytes_set += other.m_bytes_set;
    m_ops_get += other.m_ops_get;
    m_ops_set += other.m_ops_set;
    m_get_hits += other.m_get_hits;
    m_get_misses += other.m_get_misses;
    m_total_get_latency += other.m_total_get_latency;
    m_total_set_latency += other.m_total_set_latency;
}

run_stats::totals::totals() :
    m_ops_sec_set(0),
    m_ops_sec_get(0),
    m_ops_sec(0),
    m_hits_sec(0),
    m_misses_sec(0),
    m_bytes_sec_set(0),
    m_bytes_sec_get(0),
    m_bytes_sec(0),
    m_latency_set(0),
    m_latency_get(0),
    m_latency(0),
    m_bytes(0),
    m_ops_set(0),
    m_ops_get(0),
    m_ops(0)
{
}
    
void run_stats::totals::add(const run_stats::totals& other)
{
    m_ops_sec_set += other.m_ops_sec_set;
    m_ops_sec_get += other.m_ops_sec_get;
    m_ops_sec += other.m_ops_sec;
    m_hits_sec += other.m_hits_sec;
    m_misses_sec += other.m_misses_sec;
    m_bytes_sec_set += other.m_bytes_sec_set;
    m_bytes_sec_get += other.m_bytes_sec_get;
    m_bytes_sec += other.m_bytes_sec;
    m_latency_set += other.m_latency_set;
    m_latency_get += other.m_latency_get;
    m_latency += other.m_latency;
    m_bytes += other.m_bytes;
    m_ops_set += other.m_ops_set;
    m_ops_get += other.m_ops_get;
    m_ops += other.m_ops;
}

run_stats::run_stats() :
    m_cur_stats(0)
{
    memset(&m_start_time, 0, sizeof(m_start_time));
    memset(&m_end_time, 0, sizeof(m_end_time));
    memset(m_get_latency, 0, sizeof(m_get_latency));
    memset(m_set_latency, 0, sizeof(m_set_latency));    
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
    struct timeval tv;
    if (!ts) {
        gettimeofday(&tv, NULL);
        ts = &tv;
    }

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

    unsigned int msec_latency = latency / 1000;
    if (msec_latency > MAX_LATENCY_HISTOGRAM)
        msec_latency = MAX_LATENCY_HISTOGRAM;
    m_get_latency[msec_latency]++;    
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
    
    unsigned int msec_latency = latency / 1000;
    if (msec_latency > MAX_LATENCY_HISTOGRAM)
        msec_latency = MAX_LATENCY_HISTOGRAM;
    m_set_latency[msec_latency]++;
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

bool run_stats::save_csv(const char *filename)
{
    FILE *f = fopen(filename, "w");
    if (!f) {
        perror(filename);
        return false;
    }

    fprintf(f, "Per-Second Benchmark Data\n");
    fprintf(f, "Second,SET Requests,SET Average Latency,SET Total Bytes,"
               "GET Requests,GET Average Latency,GET Total Bytes,GET Misses, GET Hits\n");

    unsigned long int total_get_ops = 0;
    unsigned long int total_set_ops = 0;
    for (std::vector<one_second_stats>::iterator i = m_stats.begin();
            i != m_stats.end(); i++) {

        fprintf(f, "%u,%u,%u.%06u,%u,%u,%u.%06u,%u,%u,%u\n",
            i->m_second,
            i->m_ops_set,
            USEC_FORMAT(AVERAGE(i->m_total_set_latency, i->m_ops_set)),
            i->m_bytes_set,
            i->m_ops_get,
            USEC_FORMAT(AVERAGE(i->m_total_get_latency, i->m_ops_get)),
            i->m_bytes_get,
            i->m_get_misses,
            i->m_get_hits);

        total_get_ops += i->m_ops_get;
        total_set_ops += i->m_ops_set;
    }

    unsigned long int total_count = 0;
    fprintf(f, "\n" "Full-Test GET Latency\n");
    fprintf(f, "Latency (<= msec),Percent\n");
    for (int i = 0; i <= MAX_LATENCY_HISTOGRAM; i++) {
        if (m_get_latency[i] > 0) {
            total_count += m_get_latency[i];
            fprintf(f, "%u,%.2f\n", i, (double) total_count / total_get_ops * 100);
        }
    }
    
    fprintf(f, "\n" "Full-Test SET Latency\n");
    fprintf(f, "Latency (<= msec),Percent\n");
    total_count = 0;
    for (int i = 0; i <= MAX_LATENCY_HISTOGRAM; i++) {
        if (m_set_latency[i] > 0) {
            total_count += m_set_latency[i];
            fprintf(f, "%u,%.2f\n", i, (double) total_count / total_set_ops * 100);
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

        benchmark_debug_log("  %u: get latency=%u.%ums, set latency=%u.%ums m_ops_set/get=%u/%u, m_bytes_set/get=%u/%u, m_get_hit/miss=%u/%u\n",
            i->m_second,
            USEC_FORMAT(AVERAGE(i->m_total_get_latency, i->m_ops_get)),
            USEC_FORMAT(AVERAGE(i->m_total_set_latency, i->m_ops_set)),
            i->m_ops_set,
            i->m_ops_get,
            i->m_bytes_set,
            i->m_bytes_get,
            i->m_get_hits,
            i->m_get_misses);
    }

    for (int i = 0; i <= MAX_LATENCY_HISTOGRAM; i++) {
        if (m_get_latency[i] > 0) 
            benchmark_debug_log("  GET <= %u msec: %u\n", i, m_get_latency[i]);
    }

    for (int i = 0; i <= MAX_LATENCY_HISTOGRAM; i++) {
        if (m_set_latency[i] > 0) 
            benchmark_debug_log("  SET <= %u msec: %u\n", i, m_set_latency[i]);
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
            for (int j = 0; j < MAX_LATENCY_HISTOGRAM + 1; j++) {
                m_get_latency[j] += i->m_get_latency[j];
                m_set_latency[j] += i->m_set_latency[j];
            }            
    }

    m_totals.m_ops_sec_set /= all_stats.size();
    m_totals.m_ops_sec_get /= all_stats.size();
    m_totals.m_ops_sec /= all_stats.size();
    m_totals.m_hits_sec /= all_stats.size();
    m_totals.m_misses_sec /= all_stats.size();
    m_totals.m_bytes_sec_set /= all_stats.size();
    m_totals.m_bytes_sec_get /= all_stats.size();
    m_totals.m_bytes_sec /= all_stats.size();
    m_totals.m_latency_set /= all_stats.size();
    m_totals.m_latency_get /= all_stats.size();
    m_totals.m_latency /= all_stats.size();

}

void run_stats::merge(const run_stats& other)
{
    bool new_stats = false;

    if (!m_start_time.tv_sec)
        m_start_time = other.m_start_time;
    if (!m_end_time.tv_sec)
        m_end_time = other.m_end_time;

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
    for (int i = 0; i < MAX_LATENCY_HISTOGRAM + 1; i++) {
        m_get_latency[i] += other.m_get_latency[i];
        m_set_latency[i] += other.m_set_latency[i];
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
    unsigned int test_duration_sec = test_duration_usec / 1000000;

    result.m_ops_set = totals.m_ops_set;
    result.m_ops_get = totals.m_ops_get;
    result.m_ops = totals.m_ops_get + totals.m_ops_set;
    result.m_bytes = totals.m_bytes_get + totals.m_bytes_set;

    if (test_duration_sec == 0)
        test_duration_sec = 1;
    
    result.m_ops_sec_set = (double) totals.m_ops_set / test_duration_sec;
    if (totals.m_ops_set > 0) {
        result.m_latency_set = (double) (totals.m_total_set_latency / totals.m_ops_set) / 1000;
    } else {
        result.m_latency_set = 0;
    }
    result.m_bytes_sec_set = (double) ((totals.m_bytes_set / 1024) / test_duration_sec);

    result.m_ops_sec_get = (double) totals.m_ops_get / test_duration_sec;
    if (totals.m_ops_get > 0) {
        result.m_latency_get = (double) (totals.m_total_get_latency / totals.m_ops_get) / 1000;
    } else {
        result.m_latency_get = 0;
    }
    result.m_bytes_sec_get = (double) ((totals.m_bytes_get / 1024) / test_duration_sec);
    result.m_hits_sec = (double) totals.m_get_hits / test_duration_sec;
    result.m_misses_sec = (double) totals.m_get_misses / test_duration_sec;

    result.m_ops_sec = (double) result.m_ops / test_duration_sec;
    if (result.m_ops > 0) {
        result.m_latency = (double) ((totals.m_total_get_latency + totals.m_total_set_latency) / result.m_ops) / 1000;
    } else {
        result.m_latency = 0;
    }
    result.m_bytes_sec = (double) ((result.m_bytes / 1024) / test_duration_sec);
}

void run_stats::print(FILE *out)
{
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
           "%-6s %12.2f %12.2f %12.2f %12.05f %12.2f\n",
           "Totals",
           m_totals.m_ops_sec,
           m_totals.m_hits_sec,
           m_totals.m_misses_sec,
           m_totals.m_latency,
           m_totals.m_bytes_sec);

    fprintf(out,
            "\n\n"
            "Request Latency Distribution\n"
            "%-6s %12s %12s\n"
            "------------------------------------------------------------------------\n",
            "Type", "<= msec", "Percent");    
            
    unsigned long int total_count = 0;
    for (int i = 0; i <= MAX_LATENCY_HISTOGRAM; i++) {
        if (m_set_latency[i] > 0) {
            total_count += m_set_latency[i];
            fprintf(out, "%-6s %12u %12.2f\n",
                    "SET", i, (double) total_count / m_totals.m_ops_set * 100);
        }
    }

    fprintf(out, "---\n");
    total_count = 0;
    for (int i = 0; i <= MAX_LATENCY_HISTOGRAM; i++) {
        if (m_get_latency[i] > 0) {
            total_count += m_get_latency[i];
            fprintf(out, "%-6s %12u %12.2f\n",
                    "GET", i, (double) total_count / m_totals.m_ops_get * 100);
        }
    }    
}

