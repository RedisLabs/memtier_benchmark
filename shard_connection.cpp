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

#include "shard_connection.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"
#include "connections_manager.h"

void cluster_client_event_handler(evutil_socket_t sfd, short evtype, void *opaque)
{
    shard_connection *sc = (shard_connection *) opaque;

    assert(sc != NULL);
    assert(sc->m_sockfd == sfd);

    sc->handle_event(evtype);
}

request::request(request_type type, unsigned int size, struct timeval* sent_time, unsigned int keys)
        : m_type(type), m_size(size), m_keys(keys)
{
    if (sent_time != NULL)
        m_sent_time = *sent_time;
    else {
        gettimeofday(&m_sent_time, NULL);
    }
}

verify_request::verify_request(request_type type,
                               unsigned int size,
                               struct timeval* sent_time,
                               unsigned int keys,
                               const char *key,
                               unsigned int key_len,
                               const char *value,
                               unsigned int value_len) :
        request(type, size, sent_time, keys),
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

verify_request::~verify_request(void)
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

shard_connection::shard_connection(unsigned int id, connections_manager* conns_man, benchmark_config* config,
                                   struct event_base* event_base, abstract_protocol* abs_protocol) :
        m_sockfd(-1), m_address(NULL), m_port(NULL), m_unix_sockaddr(NULL),
        m_event(NULL), m_pending_resp(0), m_connected(false),
        m_authentication(auth_done), m_db_selection(select_done), m_cluster_slots(slots_done) {
    m_id = id;
    m_conns_manager = conns_man;
    m_config = config;
    m_event_base = event_base;

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

    m_protocol = abs_protocol->clone();
    assert(m_protocol != NULL);
    m_protocol->set_buffers(m_read_buf, m_write_buf);

    m_pipeline = new std::queue<request *>;
    assert(m_pipeline != NULL);
}

shard_connection::~shard_connection() {
    if (m_sockfd != -1) {
        close(m_sockfd);
        m_sockfd = -1;
    }

    if (m_address != NULL) {
        free(m_address);
        m_address = NULL;
    }

    if (m_port != NULL) {
        free(m_port);
        m_port = NULL;
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

    if (m_event != NULL) {
        event_free(m_event);
        m_event = NULL;
    }

    if (m_protocol != NULL) {
        delete m_protocol;
        m_protocol = NULL;
    }

    if (m_pipeline != NULL) {
        delete m_pipeline;
        m_pipeline = NULL;
    }
}

void shard_connection::setup_event() {
    int ret;

    if (!m_event) {
        m_event = event_new(m_event_base, m_sockfd, EV_WRITE,
                            cluster_client_event_handler, (void *)this);
        assert(m_event != NULL);
    } else {
        ret = event_del(m_event);
        assert(ret ==0);

        ret = event_assign(m_event, m_event_base, m_sockfd, EV_WRITE,
                           cluster_client_event_handler, (void *)this);
        assert(ret ==0);
    }

    ret = event_add(m_event, NULL);
    assert(ret == 0);
}

int shard_connection::setup_socket(struct connect_info* addr) {
    int flags;

    // clean up existing socket
    if (m_sockfd != -1)
        close(m_sockfd);

    if (m_unix_sockaddr != NULL) {
        m_sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (m_sockfd < 0) {
            return -errno;
        }
    } else {
        // initialize socket
        m_sockfd = socket(addr->ci_family, addr->ci_socktype, addr->ci_protocol);
        if (m_sockfd < 0) {
            return -errno;
        }

        // configure socket behavior
        struct linger ling = {0, 0};
        int flags = 1;
        int error = setsockopt(m_sockfd, SOL_SOCKET, SO_KEEPALIVE, (void *) &flags, sizeof(flags));
        assert(error == 0);

        error = setsockopt(m_sockfd, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling));
        assert(error == 0);

        error = setsockopt(m_sockfd, IPPROTO_TCP, TCP_NODELAY, (void *) &flags, sizeof(flags));
        assert(error == 0);
    }

    // set non-blocking behavior
    flags = 1;
    if ((flags = fcntl(m_sockfd, F_GETFL, 0)) < 0 ||
        fcntl(m_sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        benchmark_error_log("connect: failed to set non-blocking flag.\n");
        close(m_sockfd);
        m_sockfd = -1;
        return -1;
    }

    return 0;
}

int shard_connection::connect(struct connect_info* addr) {
    // set required setup commands
    m_authentication = m_config->authenticate ? auth_none : auth_done;
    m_db_selection = m_config->select_db ? select_none : select_done;

    // clean up existing buffers
    evbuffer_drain(m_read_buf, evbuffer_get_length(m_read_buf));
    evbuffer_drain(m_write_buf, evbuffer_get_length(m_write_buf));

    // setup socket
    setup_socket(addr);

    // set up event
    setup_event();

    // call connect
    if (::connect(m_sockfd,
                  m_unix_sockaddr ? (struct sockaddr *) m_unix_sockaddr : addr->ci_addr,
                  m_unix_sockaddr ? sizeof(struct sockaddr_un) : addr->ci_addrlen) == -1) {
        if (errno == EINPROGRESS || errno == EWOULDBLOCK)
            return 0;
        benchmark_error_log("connect failed, error = %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

void shard_connection::disconnect() {
    if (m_sockfd != -1) {
        close(m_sockfd);
        m_sockfd = -1;
    }

    evbuffer_drain(m_read_buf, evbuffer_get_length(m_read_buf));
    evbuffer_drain(m_write_buf, evbuffer_get_length(m_write_buf));

    int ret = event_del(m_event);
    assert(ret == 0);

    m_connected = false;

    // by default no need to send any setup request
    m_authentication = auth_done;
    m_db_selection = select_done;
    m_cluster_slots = slots_done;
}

void shard_connection::set_address_port(const char* address, const char* port) {
    m_address = strdup(address);
    m_port = strdup(port);
}

request* shard_connection::pop_req() {
    request* req = m_pipeline->front();
    m_pipeline->pop();

    m_pending_resp--;
    assert(m_pending_resp >= 0);

    return req;
}

void shard_connection::push_req(request* req) {
    m_pipeline->push(req);
    m_pending_resp++;
}

bool shard_connection::is_conn_setup_done() {
    return m_authentication == auth_done &&
           m_db_selection == select_done &&
           m_cluster_slots == slots_done;
}

void shard_connection::send_conn_setup_commands(struct timeval timestamp) {
    if (m_authentication == auth_none) {
        benchmark_debug_log("sending authentication command.\n");
        m_protocol->authenticate(m_config->authenticate);
        push_req(new request(rt_auth, 0, &timestamp, 0));
        m_authentication = auth_sent;
    }

    if (m_db_selection == select_none) {
        benchmark_debug_log("sending db selection command.\n");
        m_protocol->select_db(m_config->select_db);
        push_req(new request(rt_select_db, 0, &timestamp, 0));
        m_db_selection = select_sent;
    }

    if (m_cluster_slots == slots_none) {
        benchmark_debug_log("sending cluster slots command.\n");
        m_protocol->write_command_cluster_slots();
        push_req(new request(rt_cluster_slots, 0, &timestamp, 0));
        m_cluster_slots = slots_sent;
    }
}

void shard_connection::process_response(void)
{
    int ret;
    bool responses_handled = false;

    struct timeval now;
    gettimeofday(&now, NULL);

    while ((ret = m_protocol->parse_response()) > 0) {
        bool error = false;
        protocol_response *r = m_protocol->get_response();

        request* req = pop_req();

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
        } else if (req->m_type == rt_cluster_slots) {
            if (r->get_mbulk_value() == NULL || r->get_mbulk_value()->mbulk_array.size() == 0) {
                benchmark_error_log("cluster slot failed.\n");
                error = true;
            } else {
                // parse response
                m_conns_manager->handle_cluster_slots(r);

                m_cluster_slots = slots_done;
                benchmark_debug_log("cluster slot command successful\n");
            }
        } else {
            benchmark_debug_log("handled response (first line): %s, %d hits, %d misses\n",
                                r->get_status(),
                                r->get_hits(),
                                req->m_keys - r->get_hits());

            if (r->is_error()) {
                benchmark_error_log("error response: %s\n", r->get_status());
            }

            m_conns_manager->handle_response(now, req, r);
            m_conns_manager->inc_reqs_processed();
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
        if ((m_conns_manager->get_reqs_processed() % m_config->reconnect_interval) == 0) {
            assert(m_pipeline->size() == 0);
            benchmark_debug_log("reconnecting, m_reqs_processed = %u\n", m_conns_manager->get_reqs_processed());

            // client manage connection & disconnection of shard
            m_conns_manager->disconnect();
            ret = m_conns_manager->connect();
            assert(ret == 0);

            return;
        }
    }

    fill_pipeline();
}

void shard_connection::process_first_request() {
    m_conns_manager->set_start_time();
    fill_pipeline();
}

void shard_connection::fill_pipeline(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);

    while (!m_conns_manager->finished() && m_pipeline->size() < m_config->pipeline) {
        if (!is_conn_setup_done()) {
            send_conn_setup_commands(now);
            return;
        }

        // don't exceed requests
        if (m_conns_manager->hold_pipeline(m_id))
            break;

        // client manage requests logic
        m_conns_manager->create_request(now, m_id);
    }
}

void shard_connection::handle_event(short evtype)
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
        if (!m_conns_manager->get_reqs_processed()) {
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
    if (m_pending_resp) {
        new_evtype = EV_READ;
    }

    if (evbuffer_get_length(m_write_buf) > 0) {
        assert(!m_conns_manager->finished());
        new_evtype |= EV_WRITE;
    }

    if (new_evtype) {
        int ret = event_assign(m_event, m_event_base,
                               m_sockfd, new_evtype, cluster_client_event_handler, (void *)this);
        assert(ret == 0);

        ret = event_add(m_event, NULL);
        assert(ret == 0);
    } else if (m_conns_manager->finished()) {
        m_conns_manager->set_end_time();
    }
}

void shard_connection::send_wait_command(struct timeval* sent_time,
                                         unsigned int num_slaves, unsigned int timeout) {
    int cmd_size = 0;

    benchmark_debug_log("WAIT num_slaves=%u timeout=%u\n", num_slaves, timeout);

    cmd_size = m_protocol->write_command_wait(num_slaves, timeout);
    push_req(new request(rt_wait, cmd_size, sent_time, 0));
}

void shard_connection::send_set_command(struct timeval* sent_time, const char *key, int key_len,
                                        const char *value, int value_len, int expiry, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("SET key=[%.*s] value_len=%u expiry=%u\n",
                        key_len, key, value_len, expiry);

    cmd_size = m_protocol->write_command_set(key, key_len, value, value_len,
                                             expiry, offset);

    push_req(new request(rt_set, cmd_size, sent_time, 1));
}

void shard_connection::send_get_command(struct timeval* sent_time,
                                        const char *key, int key_len, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("GET key=[%.*s]\n", key_len, key);
    cmd_size = m_protocol->write_command_get(key, key_len, offset);

    push_req(new request(rt_get, cmd_size, sent_time, 1));
}

void shard_connection::send_mget_command(struct timeval* sent_time, const keylist* key_list) {
    int cmd_size = 0;

    const char *first_key, *last_key;
    unsigned int first_key_len, last_key_len;
    first_key = key_list->get_key(0, &first_key_len);
    last_key = key_list->get_key(key_list->get_keys_count()-1, &last_key_len);

    benchmark_debug_log("MGET %d keys [%.*s] .. [%.*s]\n",
                        key_list->get_keys_count(), first_key_len, first_key, last_key_len, last_key);

    cmd_size = m_protocol->write_command_multi_get(key_list);

    push_req(new request(rt_get, cmd_size, sent_time, key_list->get_keys_count()));
}

void shard_connection::send_verify_get_command(struct timeval* sent_time, const char *key, int key_len,
                                               const char *value, int value_len, int expiry, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("GET key=[%.*s] value_len=%u expiry=%u\n",
                        key_len, key, value_len, expiry);

    cmd_size = m_protocol->write_command_get(key, key_len, offset);

    push_req(new verify_request(rt_get, cmd_size, sent_time, 1, key, key_len, value, value_len));
}
