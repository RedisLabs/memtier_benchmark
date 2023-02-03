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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>

#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "obj_gen.h"
#include "memtier_benchmark.h"

random_generator::random_generator()
{
    set_seed(0);
}

void random_generator::set_seed(int seed)
{
    seed++; //http://stackoverflow.com/questions/27386470/srand0-and-srand1-give-the-same-results
#ifdef HAVE_RANDOM_R
    memset(&m_data_blob, 0, sizeof(m_data_blob));
    memset(m_state_array, 0, sizeof(m_state_array));

    int ret = initstate_r(seed, m_state_array, sizeof(m_state_array), &m_data_blob);
    assert(ret == 0);
#elif (defined HAVE_DRAND48)
    memset(&m_data_blob, 0, sizeof(m_data_blob));
    size_t seed_size = sizeof(seed); //get MIN size between seed and m_data_blob
    if (seed_size > sizeof(m_data_blob))
        seed_size = sizeof(m_data_blob);
    memcpy(&m_data_blob, &seed, seed_size);
#endif
}

unsigned long long random_generator::get_random()
{
    unsigned long long llrn;
#ifdef HAVE_RANDOM_R
    int32_t rn;
    // max is RAND_MAX, which is usually 2^31-1 (although can be as low as 2^16-1, which we ignore now)
    // this is fine, especially considering that random_r is a nonstandard glibc extension
    // it returns a positive int32_t, so either way the MSB is off
    int ret = random_r(&m_data_blob, &rn);
    assert(ret == 0);
    llrn = rn;
    llrn = llrn << 31;

    ret = random_r(&m_data_blob, &rn);
    assert(ret == 0);
    llrn |= rn;
#elif (defined HAVE_DRAND48)
    long rn;
    // jrand48's range is -2^31..+2^31 (i.e. all 32 bits)
    rn = jrand48(m_data_blob);
    llrn = rn;
    llrn = llrn << 32;

    rn = jrand48(m_data_blob);
    llrn |= rn & 0xffffffff; // reset the sign extension bits of negative numbers
    llrn &= 0x7FFFFFFFFFFFFFFF; // avoid any trouble from sign mismatch and negative numbers
#else
    #error no random function
#endif
    return llrn;
}

unsigned long long random_generator::get_random_max() const
{
#ifdef HAVE_RANDOM_R
    return 0x3fffffffffffffff;//62 bits
#elif (defined HAVE_DRAND48)
    return 0x7fffffffffffffff;//63 bits
#endif
}

//returns a value surrounding 0
double gaussian_noise::gaussian_distribution(const double &stddev)
{
    // Box–Muller transform (Marsaglia polar method)
    if (m_hasSpare) {
        m_hasSpare = false;
        return stddev * m_spare;
    }

    m_hasSpare = true;
    double u, v, s;
    do {
        u = (get_random() / ((double) get_random_max())) * 2 - 1;
        v = (get_random() / ((double) get_random_max())) * 2 - 1;
        s = u * u + v * v;
    } while(s >= 1 || s == 0);

    s = sqrt(-2.0 * log(s) / s);
    m_spare = v * s;
    return stddev * u * s;
}

unsigned long long gaussian_noise::gaussian_distribution_range(double stddev, double median, unsigned long long min, unsigned long long max)
{
    if (min==max)
        return min;

    unsigned long long len = max-min;

    double val;
    if (median == 0)
        median = len / 2.0 + min + 0.5;
    if (stddev == 0)
        stddev = len / 6.0;
    assert(median > min && median < max);
    do {
        val = gaussian_distribution(stddev) + median;
    } while(val < min || val > max + 1);
    return val;
}

object_generator::object_generator(size_t n_key_iterators/*= OBJECT_GENERATOR_KEY_ITERATORS*/) :
    m_data_size_type(data_size_unknown),
    m_data_size_pattern(NULL),
    m_random_data(false),
    m_expiry_min(0),
    m_expiry_max(0),
    m_key_prefix(NULL),
    m_key_min(0),
    m_key_max(0),
    m_key_stddev(0),
    m_key_median(0),
    m_value_buffer(NULL),
    m_random_fd(-1),
    m_value_buffer_size(0),
    m_value_buffer_mutation_pos(0)
{
    m_next_key.resize(n_key_iterators, 0);

    m_data_size.size_list = NULL;
}

object_generator::object_generator(const object_generator& copy) :
    m_data_size_type(copy.m_data_size_type),
    m_data_size(copy.m_data_size),
    m_data_size_pattern(copy.m_data_size_pattern),
    m_random_data(copy.m_random_data),
    m_expiry_min(copy.m_expiry_min),
    m_expiry_max(copy.m_expiry_max),
    m_key_prefix(copy.m_key_prefix),
    m_key_min(copy.m_key_min),
    m_key_max(copy.m_key_max),
    m_key_stddev(copy.m_key_stddev),
    m_key_median(copy.m_key_median),
    m_value_buffer(NULL),
    m_random_fd(-1),
    m_value_buffer_size(0),
    m_value_buffer_mutation_pos(0)
{
    if (m_data_size_type == data_size_weighted &&
        m_data_size.size_list != NULL) {
        m_data_size.size_list = new config_weight_list(*m_data_size.size_list);
    }
    alloc_value_buffer(copy.m_value_buffer);

    m_next_key.resize(copy.m_next_key.size(), 0);
}

object_generator::~object_generator()
{
    if (m_value_buffer != NULL)
        free(m_value_buffer);
    if (m_data_size_type == data_size_weighted &&
        m_data_size.size_list != NULL) {
        delete m_data_size.size_list;
    }
    if (m_random_fd != -1) {
        close(m_random_fd);
        m_random_fd = -1;
    }
}

object_generator* object_generator::clone(void)
{
    return new object_generator(*this);
}

void object_generator::set_random_seed(int seed)
{
    m_random.set_seed(seed);
}

void object_generator::alloc_value_buffer(void)
{
    unsigned int size = 0;

    if (m_value_buffer != NULL)
        free(m_value_buffer), m_value_buffer = NULL;

    if (m_data_size_type == data_size_fixed)
        size = m_data_size.size_fixed;
    else if (m_data_size_type == data_size_range)
        size = m_data_size.size_range.size_max;
    else if (m_data_size_type == data_size_weighted) {
        size = m_data_size.size_list->largest();
    }

    m_value_buffer_size = size;
    if (size > 0) {
        m_value_buffer = (char*) malloc(size);
        assert(m_value_buffer != NULL);
        if (!m_random_data) {
            memset(m_value_buffer, 'x', size);
        } else {
            if (m_random_fd == -1) {
                m_random_fd = open("/dev/urandom",  O_RDONLY);
                assert(m_random_fd != -1);
            }

            char buf1[64] = { 0 };
            char buf2[64] = { 0 };
            unsigned int buf1_idx = sizeof(buf1);
            unsigned int buf2_idx = sizeof(buf2);
            char *d = m_value_buffer;
            int ret;
            int iter = 0;
            while (d - m_value_buffer < size) {
                if (buf1_idx == sizeof(buf1)) {
                    buf1_idx = 0;
                    buf2_idx++;

                    if (buf2_idx >= sizeof(buf2)) {
                        if (iter % 20 == 0) {
                            ret = read(m_random_fd, buf1, sizeof(buf1));
                            assert(ret > -1);
                            ret = read(m_random_fd, buf2, sizeof(buf2));
                            assert(ret > -1);
                        }
                        buf2_idx = 0;
                        iter++;
                    }
                }
                *d = buf1[buf1_idx] ^ buf2[buf2_idx] ^ iter;
                d++;
                buf1_idx++;
            }
        }
    }
}

void object_generator::alloc_value_buffer(const char* copy_from)
{
    unsigned int size = 0;

    if (m_value_buffer != NULL)
        free(m_value_buffer), m_value_buffer = NULL;

    if (m_data_size_type == data_size_fixed)
        size = m_data_size.size_fixed;
    else if (m_data_size_type == data_size_range)
        size = m_data_size.size_range.size_max;
    else if (m_data_size_type == data_size_weighted)
        size = m_data_size.size_list->largest();

    m_value_buffer_size = size;
    if (size > 0) {
        m_value_buffer = (char*) malloc(size);
        assert(m_value_buffer != NULL);
        memcpy(m_value_buffer, copy_from, size);
    }
}

void object_generator::set_random_data(bool random_data)
{
    m_random_data = random_data;
}

void object_generator::set_data_size_fixed(unsigned int size)
{
    m_data_size_type = data_size_fixed;
    m_data_size.size_fixed = size;
    alloc_value_buffer();
}

void object_generator::set_data_size_range(unsigned int size_min, unsigned int size_max)
{
    m_data_size_type = data_size_range;
    m_data_size.size_range.size_min = size_min;
    m_data_size.size_range.size_max = size_max;
    alloc_value_buffer();
}

void object_generator::set_data_size_list(config_weight_list* size_list)
{
    if (m_data_size_type == data_size_weighted && m_data_size.size_list != NULL) {
        delete m_data_size.size_list;
    }
    m_data_size_type = data_size_weighted;
    m_data_size.size_list = new config_weight_list(*size_list);
    alloc_value_buffer();
}

void object_generator::set_data_size_pattern(const char* pattern)
{
    m_data_size_pattern = pattern;
}

void object_generator::set_expiry_range(unsigned int expiry_min, unsigned int expiry_max)
{
    m_expiry_min = expiry_min;
    m_expiry_max = expiry_max;
}

void object_generator::set_key_prefix(const char *key_prefix)
{
    m_key_prefix = key_prefix;
}

void object_generator::set_key_range(unsigned long long key_min, unsigned long long key_max)
{
    m_key_min = key_min;
    m_key_max = key_max;
}

void object_generator::set_key_distribution(double key_stddev, double key_median)
{
    m_key_stddev = key_stddev;
    m_key_median = key_median;
}

// return a random number between r_min and r_max
unsigned long long object_generator::random_range(unsigned long long r_min, unsigned long long  r_max)
{
    unsigned long long rn = m_random.get_random();
    return (rn % (r_max - r_min + 1)) + r_min;
}

// return a random number between r_min and r_max using normal distribution according to r_stddev
unsigned long long object_generator::normal_distribution(unsigned long long r_min, unsigned long long r_max, double r_stddev, double r_median)
{
    return m_random.gaussian_distribution_range(r_stddev, r_median, r_min, r_max);
}

unsigned long long object_generator::get_key_index(int iter)
{
    assert(iter < static_cast<int>(m_next_key.size()) && iter >= OBJECT_GENERATOR_KEY_GAUSSIAN);

    unsigned long long k;
    if (iter==OBJECT_GENERATOR_KEY_RANDOM) {
        k = random_range(m_key_min, m_key_max);
    } else if(iter==OBJECT_GENERATOR_KEY_GAUSSIAN) {
        k = normal_distribution(m_key_min, m_key_max, m_key_stddev, m_key_median);
    } else {
        if (m_next_key[iter] < m_key_min)
            m_next_key[iter] = m_key_min;
        k = m_next_key[iter];

        m_next_key[iter]++;
        if (m_next_key[iter] > m_key_max)
            m_next_key[iter] = m_key_min;
    }
    return k;
}

const char* object_generator::get_key(int iter, unsigned int *len)
{
    unsigned int l;
    m_key_index = get_key_index(iter);

    // format key
    l = snprintf(m_key_buffer, sizeof(m_key_buffer)-1,
        "%s%llu", m_key_prefix, m_key_index);
    if (len != NULL) *len = l;

    return m_key_buffer;
}

data_object* object_generator::get_object(int iter)
{
    // compute key
    (void) get_key(iter, NULL);

    // compute value
    unsigned int new_size = 0;
    get_value(m_key_index, &new_size);

    // compute expiry
    unsigned int expiry = get_expiry();

    // set object
    m_object.set_key(m_key_buffer, strlen(m_key_buffer));
    m_object.set_value(m_value_buffer, new_size);
    m_object.set_expiry(expiry);

    return &m_object;
}

const char* object_generator::get_key_prefix() {
    return m_key_prefix;
}

const char* object_generator::get_value(unsigned long long key_index, unsigned int *len) {
    // compute size
    unsigned int new_size = 0;
    if (m_data_size_type == data_size_fixed) {
        new_size = m_data_size.size_fixed;
    } else if (m_data_size_type == data_size_range) {
        if (m_data_size_pattern && *m_data_size_pattern=='S') {
            double a = (key_index-m_key_min)/static_cast<double>(m_key_max-m_key_min);
            new_size = (m_data_size.size_range.size_max-m_data_size.size_range.size_min)*a + m_data_size.size_range.size_min;
        } else {
            new_size = random_range(m_data_size.size_range.size_min > 0 ? m_data_size.size_range.size_min : 1,
                                    m_data_size.size_range.size_max);
        }
    } else if (m_data_size_type == data_size_weighted) {
        new_size = m_data_size.size_list->get_next_size();
    } else {
        assert(0);
    }

    // modify object content in case of random data
    if (m_random_data) {
        m_value_buffer[m_value_buffer_mutation_pos++]++;
        if (m_value_buffer_mutation_pos >= m_value_buffer_size)
            m_value_buffer_mutation_pos = 0;
    }

    *len = new_size;
    return m_value_buffer;
}

unsigned int object_generator::get_expiry() {
    // compute expiry
    unsigned int expiry = 0;
    if (m_expiry_max > 0) {
        expiry = random_range(m_expiry_min, m_expiry_max);
    }

    return expiry;
}

///////////////////////////////////////////////////////////////////////////

data_object::data_object() :
    m_key(NULL), m_key_len(0),
    m_value(NULL), m_value_len(0),
    m_expiry(0)
{
}

data_object::~data_object()
{
    clear();
}

void data_object::clear(void)
{
    m_key = NULL;
    m_key_len = 0;
    m_value = NULL;
    m_value_len = 0;
    m_expiry = 0;
}

void data_object::set_key(const char* key, unsigned int key_len)
{
    m_key = key;
    m_key_len = key_len;
}

const char* data_object::get_key(unsigned int* key_len)
{
    assert(key_len != NULL);
    *key_len = m_key_len;

    return m_key;
}

void data_object::set_value(const char* value, unsigned int value_len)
{
    m_value = value;
    m_value_len = value_len;
}

const char* data_object::get_value(unsigned int *value_len)
{
    assert(value_len != NULL);
    *value_len = m_value_len;

    return m_value;
}

void data_object::set_expiry(unsigned int expiry)
{
    m_expiry = expiry;
}

unsigned int data_object::get_expiry(void)
{
    return m_expiry;
}

///////////////////////////////////////////////////////////////////////////

imported_keylist::imported_keylist(const char *filename)
    : m_filename(filename)
{
}

imported_keylist::~imported_keylist()
{
    while (!m_keys.empty()) {
        free(m_keys.front());
        m_keys.erase(m_keys.begin());
    }
}

bool imported_keylist::read_keys(void)
{
    file_reader f(m_filename);

    if (!f.open_file())
        return false;
    while (!f.is_eof()) {
        memcache_item *i = f.read_item();

        if (i != NULL) {
            key* k = (key*) malloc(i->get_nkey() + sizeof(key) + 1);
            assert(k != NULL);
            k->key_len = i->get_nkey();
            memcpy(k->key_data, i->get_key(), i->get_nkey());
            delete i;

            m_keys.push_back(k);
        }
    }

    return true;
}

unsigned int imported_keylist::size(void)
{
    return m_keys.size();
}

const char* imported_keylist::get(unsigned int pos, unsigned int *len)
{
    if (pos >= m_keys.size())
        return NULL;

    key* k = m_keys[pos];
    if (len != NULL) *len = k->key_len;

    return k->key_data;
}

///////////////////////////////////////////////////////////////////////////

import_object_generator::import_object_generator(const char *filename, imported_keylist *keys, bool no_expiry) :
    m_keys(keys),
    m_reader(filename),
    m_cur_item(NULL),
    m_reader_opened(false),
    m_no_expiry(no_expiry)
{
    if (m_keys != NULL) {
        m_key_max = m_keys->size();
        m_key_min = 1;
    }
}

import_object_generator::~import_object_generator()
{
    if (m_cur_item != NULL)
        delete m_cur_item;
}

import_object_generator::import_object_generator(const import_object_generator& from) :
    object_generator(from),
    m_keys(from.m_keys),
    m_reader(from.m_reader),
    m_cur_item(NULL),
    m_no_expiry(from.m_no_expiry)
{
    if (m_keys != NULL) {
        m_key_max = m_keys->size();
        m_key_min = 1;
    }
    if (from.m_reader_opened) {
        bool r = m_reader.open_file();
        assert(r == true);
    }
}

bool import_object_generator::open_file(void)
{
    m_reader_opened = true;
    return m_reader.open_file();
}

import_object_generator* import_object_generator::clone(void)
{
    return new import_object_generator(*this);
}

const char* import_object_generator::get_key(int iter, unsigned int *len)
{
    if (m_keys == NULL) {
        return object_generator::get_key(iter, len);
    } else {
        unsigned int k = get_key_index(iter) - 1;
        return m_keys->get(k, len);
    }
}

data_object* import_object_generator::get_object(int iter)
{
    memcache_item *i = m_reader.read_item();

    if (i == NULL && m_reader.is_eof()) {
        m_reader.open_file();
        i = m_reader.read_item();
    }

    assert(i != NULL);
    if (m_cur_item != NULL) {
        delete m_cur_item;
    }
    m_cur_item = i;

    m_object.set_value(m_cur_item->get_data(), m_cur_item->get_nbytes() - 2);
    if (m_keys != NULL) {
        m_object.set_key(m_cur_item->get_key(), m_cur_item->get_nkey());
    } else {
        unsigned int tmplen;
        const char *tmpkey = object_generator::get_key(iter, &tmplen);
        m_object.set_key(tmpkey, tmplen);
    }

    // compute expiry
    int expiry = 0;
    if (!m_no_expiry) {
        if (m_expiry_max > 0) {
            expiry = random_range(m_expiry_min, m_expiry_max);
        } else {
            expiry = m_cur_item->get_exptime();
        }
        m_object.set_expiry(expiry);
    }

    return &m_object;
}

