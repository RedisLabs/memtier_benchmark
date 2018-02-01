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
    memcpy(&m_data_blob, &seed, MIN(sizeof(seed), sizeof(m_data_blob)));
#endif
}

unsigned long long random_generator::get_random()
{
    int rn;
    unsigned long long llrn;
#ifdef HAVE_RANDOM_R
    int ret = random_r(&m_data_blob, &rn);//max is RAND_MAX
    assert(ret == 0);

    llrn = rn;
    llrn = llrn << 32; // move to upper 32bits

    ret = random_r(&m_data_blob, &rn);//max is RAND_MAX
    assert(ret == 0);
    llrn |= rn; // set lower 32bits
#elif (defined HAVE_DRAND48)
    rn = nrand48(m_data_blob); // max is 1<<31
    llrn = rn;
    llrn = llrn << 32; // move to upper 32bits

    rn = nrand48(m_data_blob); // max is 1<<31
    llrn |= rn; // set lower 32bits
#else
    #error no random function
#endif
    return llrn;
}

unsigned long long random_generator::get_random_max() const
{
#ifdef HAVE_RANDOM_R
    unsigned long long rand_max = RAND_MAX;
    return (rand_max << 32) | RAND_MAX;
#elif (defined HAVE_DRAND48)
    return ((1<<31) << 32) | (1<<31);
#else
    #error no random function
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

uint32_t crc32::calc_crc32(const void *buffer, unsigned long length)
{
    const unsigned char *cp = (const unsigned char *) buffer;
    uint32_t crc = 0;

    while (length--)
        crc = (crc << 8) ^ crctab[((crc >> 24) ^ *(cp++)) & 0xFF];

    return crc;
}

const uint32_t crc32::crctab[256] = {
        0x0,
        0x04C11DB7, 0x09823B6E, 0x0D4326D9, 0x130476DC, 0x17C56B6B,
        0x1A864DB2, 0x1E475005, 0x2608EDB8, 0x22C9F00F, 0x2F8AD6D6,
        0x2B4BCB61, 0x350C9B64, 0x31CD86D3, 0x3C8EA00A, 0x384FBDBD,
        0x4C11DB70, 0x48D0C6C7, 0x4593E01E, 0x4152FDA9, 0x5F15ADAC,
        0x5BD4B01B, 0x569796C2, 0x52568B75, 0x6A1936C8, 0x6ED82B7F,
        0x639B0DA6, 0x675A1011, 0x791D4014, 0x7DDC5DA3, 0x709F7B7A,
        0x745E66CD, 0x9823B6E0, 0x9CE2AB57, 0x91A18D8E, 0x95609039,
        0x8B27C03C, 0x8FE6DD8B, 0x82A5FB52, 0x8664E6E5, 0xBE2B5B58,
        0xBAEA46EF, 0xB7A96036, 0xB3687D81, 0xAD2F2D84, 0xA9EE3033,
        0xA4AD16EA, 0xA06C0B5D, 0xD4326D90, 0xD0F37027, 0xDDB056FE,
        0xD9714B49, 0xC7361B4C, 0xC3F706FB, 0xCEB42022, 0xCA753D95,
        0xF23A8028, 0xF6FB9D9F, 0xFBB8BB46, 0xFF79A6F1, 0xE13EF6F4,
        0xE5FFEB43, 0xE8BCCD9A, 0xEC7DD02D, 0x34867077, 0x30476DC0,
        0x3D044B19, 0x39C556AE, 0x278206AB, 0x23431B1C, 0x2E003DC5,
        0x2AC12072, 0x128E9DCF, 0x164F8078, 0x1B0CA6A1, 0x1FCDBB16,
        0x018AEB13, 0x054BF6A4, 0x0808D07D, 0x0CC9CDCA, 0x7897AB07,
        0x7C56B6B0, 0x71159069, 0x75D48DDE, 0x6B93DDDB, 0x6F52C06C,
        0x6211E6B5, 0x66D0FB02, 0x5E9F46BF, 0x5A5E5B08, 0x571D7DD1,
        0x53DC6066, 0x4D9B3063, 0x495A2DD4, 0x44190B0D, 0x40D816BA,
        0xACA5C697, 0xA864DB20, 0xA527FDF9, 0xA1E6E04E, 0xBFA1B04B,
        0xBB60ADFC, 0xB6238B25, 0xB2E29692, 0x8AAD2B2F, 0x8E6C3698,
        0x832F1041, 0x87EE0DF6, 0x99A95DF3, 0x9D684044, 0x902B669D,
        0x94EA7B2A, 0xE0B41DE7, 0xE4750050, 0xE9362689, 0xEDF73B3E,
        0xF3B06B3B, 0xF771768C, 0xFA325055, 0xFEF34DE2, 0xC6BCF05F,
        0xC27DEDE8, 0xCF3ECB31, 0xCBFFD686, 0xD5B88683, 0xD1799B34,
        0xDC3ABDED, 0xD8FBA05A, 0x690CE0EE, 0x6DCDFD59, 0x608EDB80,
        0x644FC637, 0x7A089632, 0x7EC98B85, 0x738AAD5C, 0x774BB0EB,
        0x4F040D56, 0x4BC510E1, 0x46863638, 0x42472B8F, 0x5C007B8A,
        0x58C1663D, 0x558240E4, 0x51435D53, 0x251D3B9E, 0x21DC2629,
        0x2C9F00F0, 0x285E1D47, 0x36194D42, 0x32D850F5, 0x3F9B762C,
        0x3B5A6B9B, 0x0315D626, 0x07D4CB91, 0x0A97ED48, 0x0E56F0FF,
        0x1011A0FA, 0x14D0BD4D, 0x19939B94, 0x1D528623, 0xF12F560E,
        0xF5EE4BB9, 0xF8AD6D60, 0xFC6C70D7, 0xE22B20D2, 0xE6EA3D65,
        0xEBA91BBC, 0xEF68060B, 0xD727BBB6, 0xD3E6A601, 0xDEA580D8,
        0xDA649D6F, 0xC423CD6A, 0xC0E2D0DD, 0xCDA1F604, 0xC960EBB3,
        0xBD3E8D7E, 0xB9FF90C9, 0xB4BCB610, 0xB07DABA7, 0xAE3AFBA2,
        0xAAFBE615, 0xA7B8C0CC, 0xA379DD7B, 0x9B3660C6, 0x9FF77D71,
        0x92B45BA8, 0x9675461F, 0x8832161A, 0x8CF30BAD, 0x81B02D74,
        0x857130C3, 0x5D8A9099, 0x594B8D2E, 0x5408ABF7, 0x50C9B640,
        0x4E8EE645, 0x4A4FFBF2, 0x470CDD2B, 0x43CDC09C, 0x7B827D21,
        0x7F436096, 0x7200464F, 0x76C15BF8, 0x68860BFD, 0x6C47164A,
        0x61043093, 0x65C52D24, 0x119B4BE9, 0x155A565E, 0x18197087,
        0x1CD86D30, 0x029F3D35, 0x065E2082, 0x0B1D065B, 0x0FDC1BEC,
        0x3793A651, 0x3352BBE6, 0x3E119D3F, 0x3AD08088, 0x2497D08D,
        0x2056CD3A, 0x2D15EBE3, 0x29D4F654, 0xC5A92679, 0xC1683BCE,
        0xCC2B1D17, 0xC8EA00A0, 0xD6AD50A5, 0xD26C4D12, 0xDF2F6BCB,
        0xDBEE767C, 0xE3A1CBC1, 0xE760D676, 0xEA23F0AF, 0xEEE2ED18,
        0xF0A5BD1D, 0xF464A0AA, 0xF9278673, 0xFDE69BC4, 0x89B8FD09,
        0x8D79E0BE, 0x803AC667, 0x84FBDBD0, 0x9ABC8BD5, 0x9E7D9662,
        0x933EB0BB, 0x97FFAD0C, 0xAFB010B1, 0xAB710D06, 0xA6322BDF,
        0xA2F33668, 0xBCB4666D, 0xB8757BDA, 0xB5365D03, 0xB1F740B4
};

object_generator::object_generator() :
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
    for (int i = 0; i < OBJECT_GENERATOR_KEY_ITERATORS; i++)
        m_next_key[i] = 0;

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
    for (int i = 0; i < OBJECT_GENERATOR_KEY_ITERATORS; i++)
        m_next_key[i] = 0;
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

            int ret;

            ret = read(m_random_fd, m_value_buffer, size);
            assert(ret == (int)size);
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
    assert(iter < OBJECT_GENERATOR_KEY_ITERATORS && iter >= OBJECT_GENERATOR_KEY_GAUSSIAN);

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
    
    // compute size
    unsigned int new_size = 0;
    if (m_data_size_type == data_size_fixed) {
        new_size = m_data_size.size_fixed;
    } else if (m_data_size_type == data_size_range) {
        if (m_data_size_pattern && *m_data_size_pattern=='S') {
            double a = (m_key_index-m_key_min)/static_cast<double>(m_key_max-m_key_min);
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
    
    // compute expiry
    int expiry = 0;
    if (m_expiry_max > 0) {
        expiry = random_range(m_expiry_min, m_expiry_max);
    }
    
    // modify object content in case of random data
    if (m_random_data) {
        m_value_buffer[m_value_buffer_mutation_pos++]++;
        if (m_value_buffer_mutation_pos >= m_value_buffer_size)
            m_value_buffer_mutation_pos = 0;
    }

    // set object
    m_object.set_key(m_key_buffer, strlen(m_key_buffer));
    m_object.set_value(m_value_buffer, new_size);
    m_object.set_expiry(expiry);    
    
    return &m_object;
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

///////////////////////////////////////////////////////////////////////////

crc_object_generator::crc_object_generator() :
        m_crc_size(crc32::size),
        m_crc_buffer(NULL) {}

crc_object_generator::crc_object_generator(const crc_object_generator& from) :
        object_generator(from),
        m_crc_size(from.m_crc_size),
        m_actual_value_size(from.m_actual_value_size)
{
    m_crc_buffer = m_value_buffer + m_actual_value_size;
}

crc_object_generator* crc_object_generator::clone(void)
{
    return new crc_object_generator(*this);
}

void crc_object_generator::alloc_value_buffer(void)
{
    object_generator::alloc_value_buffer();
    m_actual_value_size = m_value_buffer_size - m_crc_size;
    m_crc_buffer = m_value_buffer + m_actual_value_size;
}

void crc_object_generator::alloc_value_buffer(const char* copy_from)
{
    object_generator::alloc_value_buffer(copy_from);
    m_crc_buffer = m_value_buffer + m_actual_value_size;
}

data_object* crc_object_generator::get_object(int iter)
{
    // compute key
    get_key(iter, NULL);

    // compute size
    unsigned int new_size = 0;
    if (m_data_size_type == data_size_fixed) {
        new_size = m_data_size.size_fixed;
    } else {
        assert(0);
    }

    // compute expiry
    int expiry = 0;
    if (m_expiry_max > 0) {
        expiry = random_range(m_expiry_min, m_expiry_max);
    }

    // modify object content in case of random data
    if (m_random_data) {
        m_value_buffer[m_value_buffer_mutation_pos++]++;
        if (m_value_buffer_mutation_pos >= m_actual_value_size)
            m_value_buffer_mutation_pos = 0;
    }

    //calc and set crc
    uint32_t crc = crc32::calc_crc32(m_value_buffer, m_actual_value_size);
    memcpy(m_crc_buffer, &crc, m_crc_size);

    // set object
    m_object.set_key(m_key_buffer, strlen(m_key_buffer));
    m_object.set_value(m_value_buffer, new_size);
    m_object.set_expiry(expiry);

    return &m_object;
}

unsigned int crc_object_generator::get_actual_value_size()
{
    return m_actual_value_size;
}

void crc_object_generator::reset_next_key()
{
    for (int i = 0; i < OBJECT_GENERATOR_KEY_ITERATORS; i++)
        m_next_key[i] = 0;
}

