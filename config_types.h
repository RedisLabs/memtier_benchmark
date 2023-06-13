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

struct config_quantiles {
    std::vector<float> quantile_list;
    config_quantiles();
    config_quantiles(const char *ratio_str);
    bool is_defined(void);
    inline std::vector<float>::iterator begin()  { return quantile_list.begin(); }
    inline std::vector<float>::iterator end()  { return quantile_list.end(); }
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
    char addr_buf[sizeof(struct sockaddr_storage)];
};

struct server_addr {
    server_addr(const char *hostname, int port, int resolution);
    virtual ~server_addr();

    int get_connect_info(struct connect_info *ci);
    const char* get_last_error(void) const;
protected:
    int resolve(void);
    pthread_mutex_t m_mutex;

    std::string m_hostname;
    int m_port;
    struct addrinfo *m_server_addr;
    struct addrinfo *m_used_addr;
    int m_resolution;
    int m_last_error;
};

#define KEY_PLACEHOLDER "__key__"
#define DATA_PLACEHOLDER "__data__"

enum command_arg_type {
    const_type      = 0,
    key_type        = 1,
    data_type       = 2,
    undefined_type  = 3
};

struct command_arg {
    command_arg(const char* arg, unsigned int arg_len) : type(undefined_type), data(arg, arg_len) {;}
    command_arg_type type;
    std::string data;
};

struct arbitrary_command {
    arbitrary_command(const char* cmd);

    bool set_key_pattern(const char* pattern_str);
    bool set_ratio(const char* pattern_str);
    bool split_command_to_args();

    std::vector<command_arg> command_args;
    std::string command;
    std::string command_name;
    char key_pattern;
    unsigned int keys_count;
    unsigned int ratio;
};

struct arbitrary_command_list {
private:
    std::vector<arbitrary_command> commands_list;

public:
    arbitrary_command_list() {;}

    arbitrary_command& at(size_t idx) { return commands_list.at(idx); }
    const arbitrary_command& at(std::size_t idx) const { return commands_list.at(idx); }

    // array subscript operator
    arbitrary_command& operator[](std::size_t idx) { return commands_list[idx]; }
    const arbitrary_command& operator[](std::size_t idx) const { return commands_list[idx]; }

    void add_command(const arbitrary_command& command) {
        commands_list.push_back(command);
    }

    arbitrary_command& get_last_command() {
        return commands_list.back();
    }

    size_t size() const {
        return commands_list.size();
    }

    bool is_defined() const {
        return !commands_list.empty();
    }

    unsigned int get_max_command_name_length() const {
        unsigned int max_length = 0;

        for (size_t i=0; i<size(); i++) {
            if (commands_list[i].command_name.length() > max_length) {
                max_length = commands_list[i].command_name.length();
            }
        }

        return max_length;
    }
};

#endif /* _CONFIG_TYPES_H */
