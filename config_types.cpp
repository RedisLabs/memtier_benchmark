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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

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

