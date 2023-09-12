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
#include <errno.h>

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#include <netdb.h>

#include <string>
#include <iostream>
#include <stdexcept>
#include <climits>
#include <algorithm>

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

config_quantiles::config_quantiles(){

}

config_quantiles::config_quantiles(const char *str)
{
    assert(str != NULL);

    do {
        float quantile;
        char *p = NULL;
        quantile = strtof(str, &p);
        if (!p || (*p != ',' && *p != '\0')) {
            quantile_list.clear();
            return;
        }
        str = p;
        if (*str) str++;
        quantile_list.push_back(quantile);
    } while (*str);
}

bool config_quantiles::is_defined(void)
{
    return quantile_list.size() > 0;
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


server_addr::server_addr(const char *hostname, int port, int resolution) :
    m_hostname(hostname), m_port(port), m_server_addr(NULL), m_used_addr(NULL), m_resolution(resolution), m_last_error(0)
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
    hints.ai_family = m_resolution;

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

static int hex_digit_to_int(char c) {
    if (c >= 'a' && c <= 'f') {
        return (c - 'a') + 10;
    } else if (c >= 'A' && c <= 'F') {
        return (c - 'A') + 10;
    } else if (c >= '0' && c <= '9') {
        return (c - '0');
    } else {
        return -1;
    }
}

arbitrary_command::arbitrary_command(const char* cmd) : command(cmd), key_pattern('R'), keys_count(0), ratio(1) {
    // command name is the first word in the command
    size_t pos = command.find(" ");
    if (pos == std::string::npos) {
        pos = command.size();
    }

    command_name.assign(command.c_str(), pos);
    std::transform(command_name.begin(), command_name.end(), command_name.begin(), ::toupper);
}

bool arbitrary_command::set_key_pattern(const char* pattern_str) {
    if (strlen(pattern_str) > 1) {
        return false;
    }

    if (pattern_str[0] != 'R' &&
        pattern_str[0] != 'G' &&
        pattern_str[0] != 'S' &&
        pattern_str[0] != 'P') {

        return false;
    }

    key_pattern = pattern_str[0];
    return true;
}

bool arbitrary_command::set_ratio(const char* ratio_str) {
    char *q = NULL;
    ratio = strtoul(ratio_str, &q, 10);
    if (!q || *q != '\0') {
        return false;
    }

    return true;
}

bool arbitrary_command::split_command_to_args() {
    const char *p = command.c_str();
    size_t command_len = command.length();

    char buffer[command_len];
    unsigned int buffer_len = 0;

    while (1) {
        /* skip blanks */
        while (*p && isspace(*p)) {
            p++;
        }

        if (*p) {
            /* get a token */
            bool in_quotes = 0; /* set to 1 if we are in "quotes" */
            bool in_single_quotes = 0; /* set to 1 if we are in 'single quotes' */
            bool done = 0;
            buffer_len = 0;
            //current = p;

            while (!done) {
                if (in_quotes) {
                    if (*p == '\\' && *(p + 1) == 'x' &&
                        isxdigit(*(p + 2)) &&
                        isxdigit(*(p + 3))) {

                        unsigned char byte;
                        byte = (hex_digit_to_int(*(p + 2)) * 16) +
                               hex_digit_to_int(*(p + 3));

                        buffer[buffer_len] = byte;
                        buffer_len++;
                        p += 3;
                    } else if (*p == '\\' && *(p + 1)) {
                        char c;
                        p++;

                        switch (*p) {
                            case 'n':
                                c = '\n';
                                break;

                            case 'r':
                                c = '\r';
                                break;

                            case 't':
                                c = '\t';
                                break;

                            case 'b':
                                c = '\b';
                                break;

                            case 'a':
                                c = '\a';
                                break;

                            default:
                                c = *p;
                                break;
                        }

                        buffer[buffer_len] = c;
                        buffer_len++;
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1))) {
                            goto err;
                        }

                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        buffer[buffer_len] = *p;
                        buffer_len++;
                    }
                } else if (in_single_quotes) {
                    if (*p == '\\' && *(p + 1) == '\'') {
                        p++;
                        buffer[buffer_len] = '\'';
                        buffer_len++;
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1))) {
                            goto err;
                        }

                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        buffer[buffer_len] = *p;
                        buffer_len++;
                    }
                } else {
                    switch (*p) {
                        case ' ':
                        case '\n':
                        case '\r':
                        case '\t':
                        case '\0':
                            done = 1;
                            break;

                        case '"':
                            in_quotes = 1;
                            break;

                        case '\'':
                            in_single_quotes = 1;
                            break;

                        default:
                            buffer[buffer_len] = *p;
                            buffer_len++;
                            break;
                    }
                }

                if (*p) {
                    p++;
                }
            }

            // add new arg
            command_arg arg(buffer, buffer_len);
            command_args.push_back(arg);
        } else {
            return true;
        }
    }

    err:
    return false;
}
