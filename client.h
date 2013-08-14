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

#ifndef _CLIENT_H
#define _CLIENT_H

#include <stdlib.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <vector>
#include <queue>

#include <event2/event.h>
#include <event2/buffer.h>

#include "protocol.h"

class client;               // forward decl
class client_group;         // forward decl
struct benchmark_config;

class object_generator;
class data_object;

#define MAX_LATENCY_HISTOGRAM       5000
class run_stats {
protected:
    struct one_second_stats {
        unsigned int m_second;        // from start of test
        unsigned int m_bytes_get;
        unsigned int m_bytes_set;
        unsigned int m_ops_get;
        unsigned int m_ops_set;
        unsigned int m_get_hits;
        unsigned int m_get_misses;

        unsigned long long int m_total_get_latency;
        unsigned long long int m_total_set_latency;

        one_second_stats(unsigned int second);
        void reset(unsigned int second);
        void merge(const one_second_stats& other);
    };

    friend bool one_second_stats_predicate(const run_stats::one_second_stats& a, const run_stats::one_second_stats& b);    

    struct timeval m_start_time;
    struct timeval m_end_time;

    struct totals {
        double m_ops_sec_set;
        double m_ops_sec_get;
        double m_ops_sec;

        double m_hits_sec;
        double m_misses_sec;

        double m_bytes_sec_set;
        double m_bytes_sec_get;
        double m_bytes_sec;
        
        double m_latency_set;
        double m_latency_get;
        double m_latency;

        unsigned long int m_bytes;
        unsigned long int m_ops_set;
        unsigned long int m_ops_get;
        unsigned long int m_ops;

        totals();
        void add(const totals& other);
    } m_totals;

    std::vector<one_second_stats> m_stats;
    one_second_stats m_cur_stats;

    unsigned int m_get_latency[MAX_LATENCY_HISTOGRAM+1];
    unsigned int m_set_latency[MAX_LATENCY_HISTOGRAM+1];
    void roll_cur_stats(struct timeval* ts);

public:
    run_stats();
    void set_start_time(struct timeval* start_time);
    void set_end_time(struct timeval* end_time);

    void update_get_op(struct timeval* ts, unsigned int bytes, unsigned int latency, unsigned int hits, unsigned int misses);
    void update_set_op(struct timeval* ts, unsigned int bytes, unsigned int latency);

    void aggregate_average(const std::vector<run_stats>& all_stats);
    void summarize(totals& result) const;
    void merge(const run_stats& other);
    bool save_csv(const char *filename);
    void debug_dump(void);
    void print(FILE *file);
    
    unsigned int get_duration(void);
    unsigned long int get_duration_usec(void);
    unsigned long int get_total_bytes(void);
    unsigned long int get_total_ops(void);
    unsigned long int get_total_latency(void);
 };
 
class client {
protected:
    friend void client_event_handler(evutil_socket_t sfd, short evtype, void *opaque);

    // connection related
    int m_sockfd;
    struct addrinfo* m_server_addr;
    struct sockaddr_un* m_unix_sockaddr;
    struct event* m_event;
    struct event_base* m_event_base;
    struct evbuffer *m_read_buf;
    struct evbuffer *m_write_buf;
    bool m_initialized;
    bool m_connected;
    enum authentication_state { auth_none, auth_sent, auth_done } m_authentication;
    enum select_db_state { select_none, select_sent, select_done } m_db_selection;

    // test related
    benchmark_config* m_config;
    abstract_protocol* m_protocol;
    object_generator* m_obj_gen;
    run_stats m_stats;

    // pipeline management
    enum request_type { rt_unknown, rt_set, rt_get, rt_auth, rt_select_db };
    struct request {
        request_type m_type;
        struct timeval m_sent_time;
        unsigned int m_size;
        unsigned int m_keys;

        request(request_type type, unsigned int size, struct timeval* sent_time, unsigned int keys);
        virtual ~request(void) {}
    };
    std::queue<request *> m_pipeline;

    unsigned int m_reqs_processed;      // requests processed (responses received)
    unsigned int m_set_ratio_count;     // number of sets counter (overlaps on ratio)
    unsigned int m_get_ratio_count;     // number of gets counter (overlaps on ratio)

    keylist *m_keylist;                 // used to construct multi commands

    bool setup_client(benchmark_config *config, abstract_protocol *protocol, object_generator *obj_gen);
    int connect(void);
    void disconnect(void);

    void handle_event(short evtype);
    int get_sockfd(void) { return m_sockfd; }

    virtual bool finished();
    virtual void create_request(void);
    virtual void handle_response(request *request, protocol_response *response);

    bool send_conn_setup_commands(void);
    bool is_conn_setup_done(void);
    void fill_pipeline(void);
    void process_first_request(void);
    void process_response(void);
public:
    client(client_group* group);
    client(struct event_base *event_base, benchmark_config *config, abstract_protocol *protocol, object_generator *obj_gen);
    ~client();

    bool initialized(void);
    int prepare(void);
    run_stats* get_stats(void) { return &m_stats; }
};

class verify_client : public client {
protected:
    struct verify_request : public request {
        char *m_key;
        unsigned int m_key_len;
        char *m_value;
        unsigned int m_value_len;

        verify_request(request_type type, 
            unsigned int size, 
            struct timeval* sent_time,
            unsigned int keys,
            const char *key,
            unsigned int key_len,
            const char *value,
            unsigned int value_len);
        virtual ~verify_request(void);
    };
    bool m_finished;
    unsigned long long int m_verified_keys;
    unsigned long long int m_errors;

    virtual bool finished(void);
    virtual void create_request(void);
    virtual void handle_response(request *request, protocol_response *response);
public:
    verify_client(struct event_base *event_base, benchmark_config *config, abstract_protocol *protocol, object_generator *obj_gen);
    unsigned long long int get_verified_keys(void);
    unsigned long long int get_errors(void);
};

class client_group {
protected:
    struct event_base* m_base;
    benchmark_config *m_config;
    abstract_protocol* m_protocol;
    object_generator* m_obj_gen;
    std::vector<client*> m_clients;
public:
    client_group(benchmark_config *cfg, abstract_protocol *protocol, object_generator* obj_gen);
    ~client_group();

    int create_clients(int count);
    int prepare(void);
    void run(void);

    void write_client_stats(const char *prefix);

    struct event_base *get_event_base(void) { return m_base; }
    benchmark_config *get_config(void) { return m_config; }
    abstract_protocol* get_protocol(void) { return m_protocol; }
    object_generator* get_obj_gen(void) { return m_obj_gen; }    

    unsigned long int get_total_bytes(void);
    unsigned long int get_total_ops(void);
    unsigned long int get_total_latency(void);
    unsigned long int get_duration_usec(void);

    void merge_run_stats(run_stats* target);
};


#endif	/* _CLIENT_H */
