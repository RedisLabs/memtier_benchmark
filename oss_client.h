//
// Created by yaacov on 9/7/17.
//

#ifndef MEMTIER_BENCHMARK_OSS_CLIENT_H
#define MEMTIER_BENCHMARK_OSS_CLIENT_H

#include "client.h"

// forward declaration
class oss_client;

struct shard_connection {
    shard_connection();
    ~shard_connection();

    int setup_socket(struct connect_info* addr);
    void setup_events(struct event_base* event_base);
    void setup_connection(int conn_id, oss_client* parent_client, abstract_protocol* abs_protocol);

    int id;
    oss_client* client;

    int sockfd;
    connect_info ci;

    struct evbuffer* read_buf;
    struct evbuffer* write_buf;

    struct event* read_event;
    struct event* write_event;
    bool write_event_added;

    abstract_protocol* protocol;
    std::queue<request *>* pipeline;

    int pending_reqs;
    int pending_resp;

    int min_slots_range;
    int max_slots_range;
};

class oss_client : public client {
protected:
    friend void oss_client_event_handler(evutil_socket_t sfd, short evtype, void *opaque);

    std::vector<shard_connection*> m_connections;
    short m_slots_to_shard[16384];

    bool m_write_pipe;
    short m_full_pipe_id;

    enum cluster_slots_state { slots_none, slots_sent, slots_done } m_cluster_slots_state;
    bool m_finished_set;
    int m_remain_reqs;
    bool pipes_drained;

    virtual bool send_conn_setup_commands(struct timeval timestamp);
    virtual bool is_conn_setup_done(void);
    virtual void fill_pipeline(void);
    virtual int connect(void);
    virtual void create_request(struct timeval timestamp);

    int add_connection(int min_slots_range, int max_slots_range, char* addr, char* port);
    void create_cluster_connection(protocol_response *r);
    void set_slots_to_shard();
    void handle_event(short evtype, shard_connection* sc);
    void process_response(shard_connection* sc);

public:
    oss_client(client_group* group);
    virtual ~oss_client();
    virtual bool setup_client(benchmark_config *config, abstract_protocol *protocol, object_generator *obj_gen);
    virtual int prepare(void);
};


#endif //MEMTIER_BENCHMARK_OSS_CLIENT_H
