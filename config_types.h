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

#ifndef _CONFIG_TYPES_H
#define _CONFIG_TYPES_H

#ifdef HAVE_PTHREAD_H
#include <pthread.h>
#endif

#include <netinet/in.h>

#include <vector>
#include <string>

struct config_range {
    int min;
    int max;

    config_range() : min(0), max(0) { }
    config_range(const char *range_str);
    bool is_defined(void) { return max > 0; }
};

struct config_ratio {
    unsigned int a;
    unsigned int b;

    config_ratio() : a(0), b(0) { }
    config_ratio(const char *ratio_str);
    bool is_defined(void) { return (a > 0 || b > 0); }
};

struct config_weight_list {
    struct weight_item {
        unsigned int size;
        unsigned int weight;
    };
    
    std::vector<weight_item> item_list;
    std::vector<weight_item>::iterator next_size_iter;
    unsigned int next_size_weight;

    config_weight_list();
    config_weight_list(const char* str);
    config_weight_list(const config_weight_list& copy);
    config_weight_list& operator=(const config_weight_list& rhs);
    
    bool is_defined(void);
    unsigned int largest(void);
    const char *print(char *buf, int buf_len);
    unsigned int get_next_size(void);
};

struct connect_info {
    int ci_family;
    int ci_socktype;
    int ci_protocol;
    socklen_t ci_addrlen;
    struct sockaddr *ci_addr;
    char addr_buf[sizeof(struct sockaddr_in)];
};

struct server_addr {
    enum transport_protocol {TCP=SOCK_STREAM, UDP=SOCK_DGRAM};
    server_addr(const char *hostname, int port, transport_protocol proto);
    virtual ~server_addr();

    int get_connect_info(struct connect_info *ci);
    const char* get_last_error(void) const;
protected:
    int resolve(void);
    pthread_mutex_t m_mutex;

    std::string m_hostname;
    int m_port;
    int m_protocol;
    struct addrinfo *m_server_addr;
    struct addrinfo *m_used_addr;
    int m_last_error;
};

#endif /* _CONFIG_TYPES_H */
