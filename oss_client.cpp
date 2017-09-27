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

#include "oss_client.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"

///////////////////////////////////////////////////////////////////////////
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

#define MAX_OSS_CLUSTER_HSLOT 16383
#define MAIN_CONNECTION m_connections[0]

static uint32_t calc_hslot_crc16_oss_cluster(const char *str, size_t length)
{
    uint32_t rv = (uint32_t) crc16(str, length) & MAX_OSS_CLUSTER_HSLOT;
    return rv;
}

void oss_client_event_handler(evutil_socket_t sfd, short evtype, void *opaque)
{
    shard_connection *sc = (shard_connection *) opaque;

    assert(sc != NULL);
    assert(sc->sockfd == sfd);

    sc->client->handle_event(evtype, sc);
}

shard_connection::shard_connection() : id(-1), client(NULL), sockfd(-1), read_buf(NULL), write_buf(NULL),
                                       read_event(NULL), write_event(NULL), write_event_added(false), protocol(NULL),
                                       pipeline(NULL), pending_reqs(0), pending_resp(0), min_slots_range(0), max_slots_range(0) {
}

shard_connection::~shard_connection() {
    if (sockfd != -1) {
        close(sockfd);
        sockfd = -1;
    }

    if (read_buf != NULL) {
        evbuffer_free(read_buf);
        read_buf = NULL;
    }

    if (write_buf != NULL) {
        evbuffer_free(write_buf);
        write_buf = NULL;
    }

    if (read_event != NULL) {
        event_free(read_event);
        read_event = NULL;
    }

    if (write_event != NULL) {
        event_free(write_event);
        write_event = NULL;
    }

    if (protocol != NULL) {
        delete protocol;
        protocol = NULL;
    }

    if (pipeline != NULL) {
        delete pipeline;
        pipeline = NULL;
    }
}

void shard_connection::setup_connection(int conn_id, oss_client* parent_client, abstract_protocol* abs_protocol) {
    id = conn_id;
    client = parent_client;

    read_buf = evbuffer_new();
    assert(read_buf != NULL);

    write_buf = evbuffer_new();
    assert(write_buf != NULL);

    protocol = abs_protocol->clone();
    assert(protocol != NULL);
    protocol->set_buffers(read_buf, write_buf);

    pipeline = new std::queue<request *>;
    assert(pipeline != NULL);
}

int shard_connection::setup_socket(struct connect_info* addr) {
    // clean up existing socket
    if (sockfd != -1)
        close(sockfd);

    // initialize socket
    sockfd = socket(addr->ci_family, addr->ci_socktype, addr->ci_protocol);
    if (sockfd < 0) {
        fprintf(stderr, "socket() failed: %s\n", strerror(errno));
        return -errno;
    }

    // configure socket behavior
    struct linger ling = {0, 0};
    int flags = 1;
    int error = setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    assert(error == 0);

    error = setsockopt(sockfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
    assert(error == 0);

    error = setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
    assert(error == 0);


    // set non-blocking behavior
    flags = 1;
    if ((flags = fcntl(sockfd, F_GETFL, 0)) < 0 ||
        fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        benchmark_error_log("connect: failed to set non-blocking flag.\n");
        close(sockfd);
        sockfd = -1;
        return -1;
    }

    return 0;
}

void shard_connection::setup_events(struct event_base* event_base) {
    int ret;

    // set up read event
    if (!read_event) {
        read_event = event_new(event_base, sockfd, EV_READ,
                               oss_client_event_handler, (void *)this);
        assert(read_event != NULL);
    } else {
        ret = event_del(read_event);
        assert(ret == 0);

        ret = event_assign(read_event, event_base, sockfd, EV_READ,
                           oss_client_event_handler, (void *)this);
        assert(ret == 0);
    }

    // set up write event
    if (!write_event) {
        write_event = event_new(event_base, sockfd, EV_WRITE,
                                oss_client_event_handler, (void *)this);
        assert(write_event != NULL);
    } else {
        ret = event_del(write_event);
        assert(ret == 0);

        ret = event_assign(write_event, event_base, sockfd, EV_WRITE,
                           oss_client_event_handler, (void *)this);
        assert(ret == 0);
    }
}

oss_client::oss_client(client_group* group) : client(group), m_write_pipe(false), m_full_pipe_id(-1),
                                              m_cluster_slots_state(slots_none), m_finished_set(false),
                                              m_remain_reqs(-1), pipes_drained(false)
{
}

oss_client::~oss_client() {
    for (unsigned int i=0; i<m_connections.size(); i++) {
        shard_connection* conn = m_connections[i];
        delete conn;
    }
    m_connections.clear();
}

int oss_client::prepare(void)
{
    if (!m_unix_sockaddr && (!m_config->server_addr || !MAIN_CONNECTION->protocol))
        return -1;

    if (m_config->requests)
        m_remain_reqs = m_config->requests;

    int ret = this->connect();
    if (ret < 0) {
        benchmark_error_log("prepare: failed to connect, test aborted.\n");
        return ret;
    }

    return 0;
}

bool oss_client::setup_client(benchmark_config *config, abstract_protocol *protocol, object_generator *objgen)
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

    // create main connection
    shard_connection* conn = new shard_connection();
    conn->setup_connection(m_connections.size(), this, protocol);
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
        int range = (config->key_maximum - config->key_minimum)/(config->clients*config->threads) + 1;
        int min = config->key_minimum + range*config->next_client_idx;
        int max = min+range;
        if(config->next_client_idx==(int)(config->clients*config->threads)-1)
            max = config->key_maximum; //the last clients takes the leftover
        m_obj_gen->set_key_range(min, max);
    }
    config->next_client_idx++;

    m_keylist = new keylist(m_config->multi_key_get + 1);
    assert(m_keylist != NULL);

    benchmark_debug_log("new client %p successfully set up.\n", this);
    m_initialized = true;
    return true;
}

int oss_client::connect(void) {
    struct connect_info addr;
    int ret;

    // get primary connection
    shard_connection* conn = MAIN_CONNECTION;
    assert(conn != NULL);

    // get address information
    if (m_config->server_addr->get_connect_info(&addr) != 0) {
        benchmark_error_log("connect: resolve error: %s\n", m_config->server_addr->get_last_error());
        return -1;
    }

    // setup socket
    ret = conn->setup_socket(&addr);
    if (ret < 0) {
        benchmark_error_log("connect: failed to setup socket\n");
        return -1;
    }

    // clean up existing buffers
    evbuffer_drain(conn->read_buf, evbuffer_get_length(conn->read_buf));
    evbuffer_drain(conn->write_buf, evbuffer_get_length(conn->write_buf));

    // set up read/write events
    conn->setup_events(m_event_base);

    ret = event_add(conn->read_event, NULL);
    assert(ret == 0);

    ret = event_add(conn->write_event, NULL);
    assert(ret == 0);

    // call connect
    if (::connect(conn->sockfd,
                  m_unix_sockaddr ? (struct sockaddr *) m_unix_sockaddr : addr.ci_addr,
                  m_unix_sockaddr ? sizeof(struct sockaddr_un) : addr.ci_addrlen) == -1) {
        if (errno == EINPROGRESS || errno == EWOULDBLOCK)
            return 0;
        benchmark_error_log("connect failed, error = %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

int oss_client::add_connection(int min_slots_range, int max_slots_range, char* addr, char* port) {
    shard_connection* conn = new shard_connection();
    conn->setup_connection(m_connections.size(), this, MAIN_CONNECTION->protocol);
    m_connections.push_back(conn);

    // set slots range
    conn->min_slots_range = min_slots_range;
    conn->max_slots_range = max_slots_range;

    // set socket
    struct connect_info ci;
    struct addrinfo *addr_info;
    struct addrinfo hints;
    int ret;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_INET;

    int res = getaddrinfo(addr, port, &hints, &addr_info);
    if (res != 0) {
        benchmark_error_log("connect: resolve error: %s\n", gai_strerror(res));
        return -1;
    }

    ci.ci_family = addr_info->ai_family;
    ci.ci_socktype = addr_info->ai_socktype;
    ci.ci_protocol = addr_info->ai_protocol;
    assert(addr_info->ai_addrlen <= sizeof(ci.addr_buf));
    memcpy(ci.addr_buf, addr_info->ai_addr, addr_info->ai_addrlen);
    ci.ci_addr = (struct sockaddr *) ci.addr_buf;
    ci.ci_addrlen = addr_info->ai_addrlen;

    ret = conn->setup_socket(&ci);
    if (ret < 0) {
        benchmark_error_log("connect: failed to setup socket\n");
        return -1;
    }

    // set up read/write events
    conn->setup_events(m_event_base);

    // call connect
    if (::connect(conn->sockfd,
                  m_unix_sockaddr ? (struct sockaddr *) m_unix_sockaddr : ci.ci_addr,
                  m_unix_sockaddr ? sizeof(struct sockaddr_un) : ci.ci_addrlen) == -1) {
        if (errno == EINPROGRESS || errno == EWOULDBLOCK)
            return 0;
        benchmark_error_log("connect failed, error = %s\n", strerror(errno));
        return -1;
    }

    // check connection
    int error = -1;
    socklen_t errsz = sizeof(error);

    if (getsockopt(conn->sockfd, SOL_SOCKET, SO_ERROR, (void *) &error, &errsz) == -1) {
        benchmark_error_log("connect: error getting connect response (getsockopt): %s\n", strerror(errno));
        return -1;
    }

    if (error != 0) {
        benchmark_error_log("connect: connection failed: %s\n", strerror(error));
        return -1;
    }

    return 0;
}

bool oss_client::is_conn_setup_done(void)
{
    if (client::is_conn_setup_done())
        return m_cluster_slots_state == slots_done;
    else
        return false;
}

bool oss_client::send_conn_setup_commands(struct timeval timestamp)
{
    bool sent = false;

    if (m_config->authenticate && m_authentication != auth_done) {
        if (m_authentication == auth_none) {
            benchmark_debug_log("sending authentication command.\n");
            MAIN_CONNECTION->protocol->authenticate(m_config->authenticate);
            MAIN_CONNECTION->pipeline->push(new request(rt_auth, 0, &timestamp, 0));
            m_authentication = auth_sent;
            sent = true;
        }
    }
    if (m_config->select_db && m_db_selection != select_done) {
        if (m_db_selection == select_none) {
            benchmark_debug_log("sending db selection command.\n");
            MAIN_CONNECTION->protocol->select_db(m_config->select_db);
            MAIN_CONNECTION->pipeline->push(new request(rt_select_db, 0, &timestamp, 0));
            m_db_selection = select_sent;
            sent = true;
        }
    }

    if (m_cluster_slots_state == slots_none) {
        benchmark_debug_log("sending cluster slots command.\n");
        MAIN_CONNECTION->protocol->write_command_cluster_slots();
        MAIN_CONNECTION->pipeline->push(new request(rt_cluster_slots, 0, &timestamp, 0));
        m_cluster_slots_state = slots_sent;
        sent = true;
    }

    return sent;
}

void oss_client::set_slots_to_shard() {
    unsigned int num_of_conn = m_connections.size();

    for (unsigned int i=0; i<num_of_conn; i++) {
        shard_connection* conn = m_connections[i];

        for (int j=conn->min_slots_range; j<=conn->max_slots_range; j++)
            m_slots_to_shard[j] = i;
    }
}

void oss_client::create_cluster_connection(protocol_response *r) {
    // run over response and create connections
    for (unsigned int i=0; i<r->get_mbulk_value()->mbulk_array.size(); i++) {
        // create connection
        mbulk_element* shard = r->get_mbulk_value()->mbulk_array[i];

        int min_slot = strtol(shard->mbulk_array[0]->value, NULL, 10);
        int max_slot = strtol(shard->mbulk_array[1]->value, NULL, 10);

        // hostname/ip
        mbulk_element* mbulk_addr_el = shard->mbulk_array[2]->mbulk_array[0];
        char* addr = (char*) malloc(mbulk_addr_el->value_len + 1);
        memcpy(addr, mbulk_addr_el->value, mbulk_addr_el->value_len);
        addr[mbulk_addr_el->value_len] = '\0';

        // port
        mbulk_element* mbulk_port_el = shard->mbulk_array[2]->mbulk_array[1];
        char* port = (char*) malloc(mbulk_port_el->value_len + 1);
        memcpy(port, mbulk_port_el->value, mbulk_port_el->value_len);
        port[mbulk_port_el->value_len] = '\0';

        // if it is the main connection, no need to add connection, just update range
        if (strcmp(addr, m_config->server) == 0) {
            MAIN_CONNECTION->min_slots_range = min_slot;
            MAIN_CONNECTION->max_slots_range = max_slot;
        } else {
            int res = add_connection(min_slot, max_slot, addr, port);
            assert(res == 0);
        }

        free(addr);
        free(port);
    }
}
void oss_client::process_response(shard_connection* sc) {
    int ret;
    bool responses_handled = false;

    struct timeval now;
    gettimeofday(&now, NULL);

    while ((ret = sc->protocol->parse_response()) > 0) {
        bool error = false;
        protocol_response *r = sc->protocol->get_response();

        request* req = sc->pipeline->front();
        sc->pipeline->pop();

        // if we poped from full pipeline, we can reset the full pipeline id
        if (sc->id == m_full_pipe_id)
            m_full_pipe_id = -1;

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
                create_cluster_connection(r);

                // set slot->shard array
                set_slots_to_shard();
                benchmark_debug_log("cluster slot command successful with pipe\n");
                m_cluster_slots_state = slots_done;
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

            sc->pending_resp--;
            assert(sc->pending_reqs >= 0);

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
            assert(m_pipeline->size() == 0);
            benchmark_debug_log("reconnecting, m_reqs_processed = %u\n", m_reqs_processed);
            disconnect();

            ret = connect();
            assert(ret == 0);

            return;
        }
    }

    fill_pipeline();
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

void oss_client::create_request(struct timeval timestamp)
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

        // currently wait commands goes to main connection
        shard_connection* conn = MAIN_CONNECTION;

        cmd_size = conn->protocol->write_command_wait(num_slaves, timeout);
        conn->pipeline->push(new request(rt_wait, cmd_size, &timestamp, 0));
        conn->pending_reqs++;
        m_remain_reqs--;

        if (conn->pipeline->size() >= m_config->pipeline) {
            m_write_pipe = true;
            m_full_pipe_id = 0;
        }
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

        int slot = calc_hslot_crc16_oss_cluster(key, key_len);
        shard_connection* conn = m_connections[m_slots_to_shard[slot]];

        cmd_size = conn->protocol->write_command_set(key, key_len, value, value_len,
                                                     obj->get_expiry(), m_config->data_offset);
        conn->pipeline->push(new request(rt_set, cmd_size, &timestamp, 1));
        conn->pending_reqs++;
        m_remain_reqs--;

        if (conn->pipeline->size() >= m_config->pipeline) {
            m_write_pipe = true;
            m_full_pipe_id = conn->id;
        }
    } else if (m_get_ratio_count < m_config->ratio.b) {
        // get command
        int iter = obj_iter_type(m_config, 2);

        // currently multi get not supported
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
            m_pipeline->push(new request(rt_get, cmd_size, &timestamp, m_keylist->get_keys_count()));
        } else {
            unsigned int keylen;
            const char *key = m_obj_gen->get_key(iter, &keylen);
            assert(key != NULL);
            assert(keylen > 0);

            int slot = calc_hslot_crc16_oss_cluster(key, keylen);
            shard_connection* conn = m_connections[m_slots_to_shard[slot]];

            benchmark_debug_log("GET key=[%.*s]\n", keylen, key);

            m_get_ratio_count++;

            cmd_size = conn->protocol->write_command_get(key, keylen, m_config->data_offset);
            conn->pipeline->push(new request(rt_get, cmd_size, &timestamp, 1));
            conn->pending_reqs++;
            m_remain_reqs--;

            // update pipe full information
            if (conn->pipeline->size() >= m_config->pipeline) {
                m_write_pipe = true;
                m_full_pipe_id = conn->id;
            }
        }
    } else {
        // overlap counters
        m_get_ratio_count = m_set_ratio_count = 0;
    }
}

void oss_client::fill_pipeline(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);

    while (!finished() && m_full_pipe_id == -1) {
        if (!is_conn_setup_done()) {
            send_conn_setup_commands(now);
            return;
        }

        // don't exceed requests
        if (!m_remain_reqs)
            break;

        // if we have reconnect_interval stop enlarging the pipeline
        // on time
        if (m_config->reconnect_interval) {
            if ((m_reqs_processed % m_config->reconnect_interval) + m_pipeline->size() >= m_config->reconnect_interval)
                return;
        }

        create_request(now);
    }
}

void oss_client::handle_event(short evtype, shard_connection* sc)
{
    // connect() returning to us?  normally we expect EV_WRITE, but for UNIX domain
    // sockets we workaround since connect() returned immediately, but we don't want
    // to do any I/O from the client::connect() call...
    if (!m_connected && (evtype == EV_WRITE || m_unix_sockaddr != NULL)) {
        int error = -1;
        socklen_t errsz = sizeof(error);

        if (getsockopt(sc->sockfd, SOL_SOCKET, SO_ERROR, (void *) &error, &errsz) == -1) {
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
    if ((evtype & EV_WRITE) == EV_WRITE && evbuffer_get_length(sc->write_buf) > 0) {
        // update read event
        if (sc->pending_resp == 0) {
            int ret = event_add(sc->read_event, NULL);
            assert(ret == 0);
        }

        // update event handling parameters
        sc->pending_resp += sc->pending_reqs;
        sc->pending_reqs = 0;
        sc->write_event_added = false;

        // write buffer
        if (evbuffer_write(sc->write_buf, sc->sockfd) < 0) {
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
            ret = evbuffer_read(sc->read_buf, sc->sockfd, -1);
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

        if (evbuffer_get_length(sc->read_buf) > 0) {
            process_response(sc);

            if (sc->pending_resp > 0) {
                ret = event_add(sc->read_event, NULL);
                assert(ret == 0);
            }

            // process_response may have disconnected, in which case
            // we just abort and wait for libevent to call us back sometime
            if (!m_connected) {
                return;
            }
        }
    }

    // in case we pushed all requests, just drain all pipes
    if (!m_remain_reqs && !pipes_drained) {
        for (unsigned int i=0; i<m_connections.size(); i++) {
            int ret;
            shard_connection* conn = m_connections[i];

            if (!conn->pending_reqs || conn->write_event_added)
                continue;

            ret = event_add(conn->write_event, NULL);
            assert(ret == 0);

        }

        // no need to add anymore write events
        pipes_drained = true;
        m_write_pipe = false;
    }

    // update write event
    if (m_write_pipe) {
        assert(finished() == false);

        int ret;
        shard_connection* conn = m_connections[m_full_pipe_id];

        if (!conn->write_event_added) {
            ret = event_add(conn->write_event, NULL);
            assert(ret == 0);
        }

        m_write_pipe = false;
    }

    if (finished() && !m_finished_set) {
        // update only once
        benchmark_debug_log("nothing else to do, test is finished.\n");
        fprintf(stderr, "ydbg: num of %d\n", m_remain_reqs);
        m_stats.set_end_time(NULL);
        m_finished_set = true;
    }
}
