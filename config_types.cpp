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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sstream>
#include <iterator>
#include <sys/sysinfo.h>

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#include <netdb.h>

#include <string>
#include <stdexcept>

#include "config_types.h"

config_range::config_range(const char *range_str) :
    min(0), max(0)
{
    assert(range_str != NULL);

    char *p = NULL;
    min = strtoul(range_str, &p, 10);
    if (!p || *p != '-') {
        min = max = 0;
        return;
    }

    char *q = NULL;
    max = strtoul(p + 1, &q, 10);
    if (!q || *q != '\0') {
        min = max = 0;
        return;
    }

    if (min > max) {
        int tmp = min;
        min = max;
        max = tmp;
    }
}

config_ratio::config_ratio(const char *ratio_str) :
    a(0), b(0)
{
    assert(ratio_str != NULL);

    char *p = NULL;
    a = strtoul(ratio_str, &p, 10);
    if (!p || *p != ':') {
        a = b = 0;
        return;
    }

    char *q = NULL;
    b = strtoul(p + 1, &q, 10);
    if (!q || *q != '\0') {
        a = b = 0;
        return;
    }    
}

config_weight_list::config_weight_list() :
    next_size_weight(0)
{
}

config_weight_list::config_weight_list(const config_weight_list& copy) :
     next_size_weight(0)
{
    for (std::vector<weight_item>::const_iterator i = copy.item_list.begin(); i != copy.item_list.end(); i++) {
        const weight_item wi = *i;
        item_list.push_back(wi);
    }    
    next_size_iter = item_list.begin();
}

config_weight_list& config_weight_list::operator=(const config_weight_list& rhs)
{
    if (this == &rhs)
        return *this;

    next_size_weight = rhs.next_size_weight;
    for (std::vector<weight_item>::const_iterator i = rhs.item_list.begin(); i != rhs.item_list.end(); i++) {
        const weight_item wi = *i;
        item_list.push_back(wi);
    }    
    next_size_iter = item_list.begin();
    return *this;
}

config_weight_list::config_weight_list(const char *str) :
    next_size_weight(0)
{
    assert(str != NULL);

    do {
        struct weight_item w;
        char *p = NULL;
        w.size = strtoul(str, &p, 10);
        if (!p || *p != ':') {
            item_list.clear();
            return;
        }

        str = p + 1;
        w.weight = strtoul(str, &p, 10);
        if (!p || (*p != ',' && *p != '\0')) {
            item_list.clear();
            return;
        }

        str = p;
        if (*str) str++;
        item_list.push_back(w);
    } while (*str);

    next_size_iter = item_list.begin();
}

bool config_weight_list::is_defined(void)
{
    if (item_list.size() > 0)
        return true;
    return false;
}

unsigned int config_weight_list::largest(void)
{
    unsigned int largest = 0;
    for (std::vector<weight_item>::iterator i = item_list.begin(); i != item_list.end(); i++) {
        if (i->size > largest)
            largest = i->size;
    }

    return largest;
}

unsigned int config_weight_list::get_next_size(void)
{
    while (next_size_weight >= next_size_iter->weight) {
        next_size_iter++;
        next_size_weight = 0;
        if (next_size_iter == item_list.end()) {
            next_size_iter = item_list.begin();
        }
    }

    next_size_weight++;
    return next_size_iter->size;
}

const char* config_weight_list::print(char *buf, int buf_len)
{
    const char* start = buf;
    assert(buf != NULL && buf_len > 0);

    *buf = '\0';
    for (std::vector<weight_item>::iterator i = item_list.begin(); i != item_list.end(); i++) {
        int n = snprintf(buf, buf_len, "%s%u:%u", 
                i != item_list.begin() ? "," : "", i->size, i->weight);
        buf += n;
        buf_len -= n;
        if (!buf_len)
            return NULL;
    }

    return start;
}


server_addr::server_addr(const char *hostname, int port) :
    m_hostname(hostname), m_port(port), m_server_addr(NULL), m_used_addr(NULL), m_last_error(0)
{
    int error = resolve();

    if (error != 0)
        throw std::runtime_error(std::string(gai_strerror(error)));

    pthread_mutex_init(&m_mutex, NULL);
}

server_addr::~server_addr()
{
    if (m_server_addr) {
        freeaddrinfo(m_server_addr);
        m_server_addr = NULL;
    }

    pthread_mutex_destroy(&m_mutex);
}

int server_addr::resolve(void)
{
    char port_str[20];
    struct addrinfo hints;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_INET;      // Don't play with IPv6 for now...

    snprintf(port_str, sizeof(port_str)-1, "%u", m_port);
    m_last_error = getaddrinfo(m_hostname.c_str(), port_str, &hints, &m_server_addr);
    return m_last_error;
}

int server_addr::get_connect_info(struct connect_info *ci)
{
    pthread_mutex_lock(&m_mutex);
    if (m_used_addr)
        m_used_addr = m_used_addr->ai_next;
    if (!m_used_addr) {
        if (m_server_addr) {
            freeaddrinfo(m_server_addr);
            m_server_addr = NULL;
        }
        if (resolve() == 0) {
            m_used_addr = m_server_addr;
        } else {
            m_used_addr = NULL;
        }
    }

    if (m_used_addr) {
        ci->ci_family = m_used_addr->ai_family;
        ci->ci_socktype = m_used_addr->ai_socktype;
        ci->ci_protocol = m_used_addr->ai_protocol;
        assert(m_used_addr->ai_addrlen <= sizeof(ci->addr_buf));
        memcpy(ci->addr_buf, m_used_addr->ai_addr, m_used_addr->ai_addrlen);
        ci->ci_addr = (struct sockaddr *) ci->addr_buf;
        ci->ci_addrlen = m_used_addr->ai_addrlen;
    }
    pthread_mutex_unlock(&m_mutex);
    return m_last_error;
}

const char* server_addr::get_last_error(void) const
{
    return gai_strerror(m_last_error);
}

std::vector<std::string> config_cpu_list::parse_str(std::string str, char delimeter) const
{
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);

    while (std::getline(tokenStream, token, delimeter)) {
        tokens.push_back(token);
    }

    return tokens;
}

unsigned int config_cpu_list::parse_cpu(std::string str) const
{
    if (str.empty())
        throw illegal_list_format();

    char *endptr = NULL;
    unsigned int base = 10;
    int cpu = strtol(str.c_str(), &endptr, base);
    if (*endptr != '\0')
        throw illegal_list_format();

    return cpu;
}

void config_cpu_list::add_cpu(unsigned int cpu)
{
    if (cpu_list.find(cpu) != cpu_list.end())
        throw duplicate_cpu(cpu);

    cpu_list.insert(cpu);
}

void config_cpu_list::add_cpu_range(unsigned int cpu1, unsigned int cpu2)
{
    if (cpu1 > cpu2)
        throw illegal_range(cpu1, cpu2);

    for (unsigned int i = cpu1; i <= cpu2; ++i) {
        add_cpu(i);
    }
}

void config_cpu_list::check_list_is_legal() const
{
    unsigned int max_core = get_nprocs();

    for (std::set<unsigned int>::const_iterator it = cpu_list.begin(); it != cpu_list.end(); ++it) {
        if (*it >= max_core)
            throw bad_cpu(*it);
    }
}

config_cpu_list::config_cpu_list(unsigned int cores)
{
    for (unsigned int i = 0; i < cores; ++i)
        cpu_list.insert(i);

    next_cpu = cpu_list.begin();
}

config_cpu_list::config_cpu_list(std::string str)
{
    std::vector<std::string> tokens = parse_str(str, ',');

    for (std::vector<std::string>::const_iterator it = tokens.begin(); it != tokens.end(); ++it) {
        std::vector<std::string> split_token = parse_str(*it, '-');
        if (split_token.size() == 1) {
            unsigned int cpu = parse_cpu(split_token[0]);
            add_cpu(cpu);
        }
        else if (split_token.size() == 2) {
            unsigned int cpu1 = parse_cpu(split_token[0]);
            unsigned int cpu2 = parse_cpu(split_token[1]);
            add_cpu_range(cpu1, cpu2);
        }
        else
            throw illegal_list_format();

    }

    check_list_is_legal();
    next_cpu = cpu_list.begin();
}

config_cpu_list::config_cpu_list(const config_cpu_list& copy) :
        cpu_list(copy.cpu_list)
{
    next_cpu = cpu_list.begin();
}

config_cpu_list& config_cpu_list::operator=(const config_cpu_list& cl)
{
    if (this == &cl)
        return *this;

    cpu_list = cl.cpu_list;
    next_cpu = cpu_list.begin();
    return *this;
}

bool config_cpu_list::is_defined() const
{
    return !cpu_list.empty();
}

std::set<unsigned int> config_cpu_list::get_next_cpu()
{
    if (next_cpu == cpu_list.end())
        next_cpu = cpu_list.begin();

    std::set<unsigned int> next;
    next.insert(*(next_cpu++));
    return next;
}

std::set<unsigned int> config_cpu_list::get_cpu_list() const
{
    return std::set<unsigned int>(cpu_list);
}

const char* config_cpu_list::print(char* buf, unsigned int size) const
{
    if (!is_defined())
        return "";

    std::stringstream result;
    std::copy(cpu_list.begin(), cpu_list.end(), std::ostream_iterator<unsigned int>(result, ","));
    std::string str_res = result.str();
    str_res.erase(str_res.size()-1);

    strncpy(buf, str_res.c_str(), size);
    return buf;
}
