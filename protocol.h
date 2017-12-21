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

#ifndef _PROTOCOL_H
#define _PROTOCOL_H

#include <event2/buffer.h>
#include <vector>

struct mbulk_element {
    mbulk_element() : value(NULL), value_len(0) {;}
    mbulk_element(char* val, unsigned int len) : value(val), value_len(len) {;}

    void free_mbulk() {
        if (value != NULL) {
            free((void *) value);
        } else {
            for (unsigned int i=0; i<mbulk_array.size(); i++) {
                mbulk_array[i]->free_mbulk();
                free((void *)mbulk_array[i]);
            }
            mbulk_array.clear();
        }
    }

    char* value;
    unsigned int value_len;

    std::vector<mbulk_element*> mbulk_array;
};

struct mbulk_level {
    mbulk_level (int count, mbulk_element* mbulk_el) : mbulk_count(count), mbulk(mbulk_el) {;}

    unsigned int mbulk_count;
    mbulk_element* mbulk;
};

typedef std::vector<mbulk_level*> mbulk_level_array;

class protocol_response {
protected:
    const char *m_status;
    const char *m_value;
    mbulk_element *m_mbulk_value;
    unsigned int m_value_len;
    unsigned int m_total_len;
    unsigned int m_hits;
    bool m_error;

public:
    protocol_response();
    virtual ~protocol_response();

    void set_status(const char *status);
    const char *get_status(void);

    void set_error(bool error);
    bool is_error(void);

    void set_value(const char *value, unsigned int value_len);
    const char *get_value(unsigned int *value_len);

    void set_total_len(unsigned int total_len);
    unsigned int get_total_len(void);

    void incr_hits(void);
    unsigned int get_hits(void);

    void clear();

    void set_mbulk_value(mbulk_element* element);
    void add_mbulk_array(mbulk_level_array* mbulk_level, mbulk_element* element, int count);
    void add_mbulk_value(mbulk_level_array* mbulk_level, mbulk_element* element);
    const mbulk_element* get_mbulk_value();
};

class keylist {
protected:
    struct key_entry {
        char *key_ptr;
        unsigned int key_len;
    };
    
    char *m_buffer;
    char *m_buffer_ptr;
    unsigned int m_buffer_size;
    
    key_entry *m_keys;
    unsigned int m_keys_size;
    unsigned int m_keys_count;
    
public:
    keylist(unsigned int max_keys);
    ~keylist();

    bool add_key(const char *key, unsigned int key_len);
    unsigned int get_keys_count(void) const;
    const char *get_key(unsigned int index, unsigned int *key_len) const;

    void clear(void);
};

class abstract_protocol {
protected:
    struct evbuffer* m_read_buf;
    struct evbuffer* m_write_buf;

    bool m_keep_value;
    struct protocol_response m_last_response;
public:
    abstract_protocol();
    virtual ~abstract_protocol();
    virtual abstract_protocol* clone(void) = 0;
    void set_buffers(struct evbuffer* read_buf, struct evbuffer* write_buf);    
    void set_keep_value(bool flag);

    virtual int select_db(int db) = 0;
    virtual int authenticate(const char *credentials) = 0;
    virtual int write_command_cluster_slots() = 0;
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset) = 0;
    virtual int write_command_get(const char *key, int key_len, unsigned int offset) = 0;
    virtual int write_command_multi_get(const keylist *keylist) = 0;
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout) = 0;
    virtual int parse_response() = 0;

    struct protocol_response* get_response(void) { return &m_last_response; }
};

class abstract_protocol *protocol_factory(const char *proto_name);

#endif  /* _PROTOCOL_H */
