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

#ifndef _OBJ_GEN_H
#define _OBJ_GEN_H

#include <vector>
#include "file_io.h"

struct random_data;
struct config_weight_list;

class data_object {
protected:    
    const char *m_key;
    unsigned int m_key_len;
    const char *m_value;
    unsigned int m_value_len;
    unsigned int m_expiry;
public:
    data_object();
    ~data_object();

    void clear(void);
    void set_key(const char* key, unsigned int key_len);
    const char* get_key(unsigned int* key_len);
    void set_value(const char* value, unsigned int value_len);
    const char* get_value(unsigned int* value_len);
    void set_expiry(unsigned int expiry);
    unsigned int get_expiry(void);    
};

#define OBJECT_GENERATOR_KEY_ITERATORS  2

class object_generator {
public:
    enum data_size_type { data_size_unknown, data_size_fixed, data_size_range, data_size_weighted };
protected:
    data_size_type m_data_size_type;
    union {
        unsigned int size_fixed;
        struct {
            unsigned int size_min;
            unsigned int size_max;
        } size_range;
        config_weight_list* size_list;
    } m_data_size;
    bool m_random_data;
    unsigned int m_expiry_min;
    unsigned int m_expiry_max;
    const char *m_key_prefix;
    unsigned int m_key_min;
    unsigned int m_key_max;
    data_object m_object;

    unsigned int m_next_key[OBJECT_GENERATOR_KEY_ITERATORS];

    char m_key_buffer[250];
    char *m_value_buffer;
    int m_random_fd;

#ifdef HAVE_RANDOM_R
    struct random_data m_random_data_blob;
    char m_random_state_array[512];
#else
#ifdef HAVE_DRAND48
    unsigned short m_random_data_blob[3];
#endif
#endif
    
    void alloc_value_buffer(void);
    void random_init(void);
    unsigned int random_range(unsigned int r_min, unsigned int r_max);
    unsigned int get_key_index(unsigned int iter);
public:    
    object_generator();
    object_generator(const object_generator& copy);
    virtual ~object_generator();
    virtual object_generator* clone(void);

    void set_random_data(bool random_data);
    void set_data_size_fixed(unsigned int size);
    void set_data_size_range(unsigned int size_min, unsigned int size_max);
    void set_data_size_list(config_weight_list* data_size_list);
    void set_expiry_range(unsigned int expiry_min, unsigned int expiry_max);
    void set_key_prefix(const char *key_prefix);    
    void set_key_range(unsigned int key_min, unsigned int key_max);

    virtual const char* get_key(unsigned int iter, unsigned int *len);
    virtual data_object* get_object(unsigned int iter);
};

class imported_keylist;
class memcache_item;

class imported_keylist {
protected:
    struct key {
        unsigned int key_len;
        char key_data[0];
    };
    const char *m_filename;
    std::vector<key*> m_keys;
public:
    imported_keylist(const char *filename);
    ~imported_keylist();

    bool read_keys(void);
    unsigned int size();
    const char* get(unsigned int pos, unsigned int *len);
};

class import_object_generator : public object_generator {
protected:
    imported_keylist* m_keys;
    file_reader m_reader;
    memcache_item* m_cur_item;
    bool m_reader_opened;
    bool m_no_expiry;
public:
    import_object_generator(const char *filename, imported_keylist* keys, bool no_expiry);
    import_object_generator(const import_object_generator& from);
    virtual ~import_object_generator();
    virtual import_object_generator* clone(void);

    virtual const char* get_key(unsigned int iter, unsigned int *len);
    virtual data_object* get_object(unsigned int iter);

    bool open_file(void);
};

#endif /* _OBJ_GEN_H */
