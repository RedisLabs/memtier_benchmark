/*
 * Copyright (C) 2011-2026 Redis Labs Ltd.
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
#include <errno.h>
#include <fcntl.h>

#include "statsd.h"

statsd_client::statsd_client() :
    m_socket(-1),
    m_enabled(false),
    m_initialized(false)
{
    memset(&m_server_addr, 0, sizeof(m_server_addr));
    memset(m_prefix, 0, sizeof(m_prefix));
}

statsd_client::~statsd_client()
{
    close();
}

bool statsd_client::init(const char* host, unsigned short port, const char* prefix)
{
    if (host == NULL || host[0] == '\0') {
        m_enabled = false;
        return false;
    }

    // Create UDP socket
    m_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (m_socket < 0) {
        fprintf(stderr, "statsd: failed to create UDP socket: %s\n", strerror(errno));
        return false;
    }

    // Set socket to non-blocking to avoid blocking on send
    int flags = fcntl(m_socket, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(m_socket, F_SETFL, flags | O_NONBLOCK);
    }

    // Resolve hostname
    struct hostent* server = gethostbyname(host);
    if (server == NULL) {
        fprintf(stderr, "statsd: failed to resolve host '%s'\n", host);
        ::close(m_socket);
        m_socket = -1;
        return false;
    }

    // Setup server address
    memset(&m_server_addr, 0, sizeof(m_server_addr));
    m_server_addr.sin_family = AF_INET;
    m_server_addr.sin_port = htons(port);
    memcpy(&m_server_addr.sin_addr.s_addr, server->h_addr, server->h_length);

    // Store prefix
    if (prefix != NULL && prefix[0] != '\0') {
        snprintf(m_prefix, sizeof(m_prefix) - 1, "%s.", prefix);
    } else {
        m_prefix[0] = '\0';
    }

    m_enabled = true;
    m_initialized = true;

    fprintf(stderr, "statsd: initialized, sending metrics to %s:%u with prefix '%s'\n",
            host, port, prefix ? prefix : "");

    return true;
}

void statsd_client::close()
{
    if (m_socket >= 0) {
        ::close(m_socket);
        m_socket = -1;
    }
    m_enabled = false;
    m_initialized = false;
}

void statsd_client::send_metric(const char* name, const char* value, const char* type)
{
    if (!is_enabled() || m_socket < 0) {
        return;
    }

    char buffer[512];
    int len = snprintf(buffer, sizeof(buffer) - 1, "%s%s:%s|%s", m_prefix, name, value, type);

    if (len > 0 && len < (int)sizeof(buffer)) {
        // Send UDP packet (fire and forget - don't check return value)
        sendto(m_socket, buffer, len, 0,
               (struct sockaddr*)&m_server_addr, sizeof(m_server_addr));
    }
}

void statsd_client::gauge(const char* name, double value)
{
    char val_str[64];
    snprintf(val_str, sizeof(val_str) - 1, "%.6f", value);
    send_metric(name, val_str, "g");
}

void statsd_client::gauge(const char* name, long value)
{
    char val_str[64];
    snprintf(val_str, sizeof(val_str) - 1, "%ld", value);
    send_metric(name, val_str, "g");
}

void statsd_client::timing(const char* name, double value_ms)
{
    char val_str[64];
    snprintf(val_str, sizeof(val_str) - 1, "%.3f", value_ms);
    send_metric(name, val_str, "ms");
}

void statsd_client::counter(const char* name, long value)
{
    char val_str[64];
    snprintf(val_str, sizeof(val_str) - 1, "%ld", value);
    send_metric(name, val_str, "c");
}

void statsd_client::histogram(const char* name, double value)
{
    char val_str[64];
    snprintf(val_str, sizeof(val_str) - 1, "%.6f", value);
    send_metric(name, val_str, "h");
}

