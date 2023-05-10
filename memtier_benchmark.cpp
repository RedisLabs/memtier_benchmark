/*
 * Copyright (C) 2011-2019 Redis Labs Ltd.
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

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <getopt.h>
#include <assert.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>

#ifdef USE_TLS
#include <openssl/crypto.h>
#include <openssl/conf.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/rand.h>
#endif

#include <stdexcept>

#include "client.h"
#include "JSON_handler.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"


static int log_level = 0;
void benchmark_log_file_line(int level, const char *filename, unsigned int line, const char *fmt, ...)
{
    if (level > log_level)
        return;

    va_list args;
    char fmtbuf[1024];

    snprintf(fmtbuf, sizeof(fmtbuf)-1, "%s:%u: ", filename, line);
    strcat(fmtbuf, fmt);

    va_start(args, fmt);
    vfprintf(stderr, fmtbuf, args);
    va_end(args);
}

void benchmark_log(int level, const char *fmt, ...)
{
    if (level > log_level)
        return;

    va_list args;

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
}

bool is_redis_protocol(enum PROTOCOL_TYPE type) {
    return (type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3);
}

static const char * get_protocol_name(enum PROTOCOL_TYPE type) {
    if (type == PROTOCOL_REDIS_DEFAULT) return "redis";
    else if (type == PROTOCOL_RESP2) return "resp2";
    else if (type == PROTOCOL_RESP3) return "resp3";
    else if (type == PROTOCOL_MEMCACHE_TEXT) return "memcache_text";
    else if (type == PROTOCOL_MEMCACHE_BINARY) return "memcache_binary";
    else return "none";
}

static void config_print(FILE *file, struct benchmark_config *cfg)
{
    char tmpbuf[512];

    fprintf(file,
        "server = %s\n"
        "port = %u\n"
        "unix socket = %s\n"
        "address family = %s\n"
        "protocol = %s\n"
#ifdef USE_TLS
        "tls = %s\n"
        "cert = %s\n"
        "key = %s\n"
        "cacert = %s\n"
        "tls_skip_verify = %s\n"
        "sni = %s\n"
#endif
        "out_file = %s\n"
        "client_stats = %s\n"
        "run_count = %u\n"
        "debug = %u\n"
        "requests = %llu\n"
        "clients = %u\n"
        "threads = %u\n"
        "test_time = %u\n"
        "ratio = %u:%u\n"
        "pipeline = %u\n"
        "data_size = %u\n"
        "data_offset = %u\n"
        "random_data = %s\n"
        "data_size_range = %u-%u\n"
        "data_size_list = %s\n"
        "data_size_pattern = %s\n"
        "expiry_range = %u-%u\n"
        "data_import = %s\n"
        "data_verify = %s\n"
        "verify_only = %s\n"
        "generate_keys = %s\n"
        "key_prefix = %s\n"
        "key_minimum = %llu\n"
        "key_maximum = %llu\n"
        "key_pattern = %s\n"
        "key_stddev = %f\n"
        "key_median = %f\n"
        "reconnect_interval = %u\n"
        "multi_key_get = %u\n"
        "authenticate = %s\n"
        "select-db = %d\n"
        "no-expiry = %s\n"
        "wait-ratio = %u:%u\n"
        "num-slaves = %u-%u\n"
        "wait-timeout = %u-%u\n"
        "json-out-file = %s\n",
        cfg->server,
        cfg->port,
        cfg->unix_socket,
        cfg->resolution == AF_UNSPEC ? "Unspecified" : cfg->resolution == AF_INET ? "AF_INET" : "AF_INET6",
        get_protocol_name(cfg->protocol),
#ifdef USE_TLS
        cfg->tls ? "yes" : "no",
        cfg->tls_cert,
        cfg->tls_key,
        cfg->tls_cacert,
        cfg->tls_skip_verify ? "yes" : "no",
        cfg->tls_sni,
#endif
        cfg->out_file,
        cfg->client_stats,
        cfg->run_count,
        cfg->debug,
        cfg->requests,
        cfg->clients,
        cfg->threads,
        cfg->test_time,
        cfg->ratio.a, cfg->ratio.b,
        cfg->pipeline,
        cfg->data_size,
        cfg->data_offset,
        cfg->random_data ? "yes" : "no",
        cfg->data_size_range.min, cfg->data_size_range.max,
        cfg->data_size_list.print(tmpbuf, sizeof(tmpbuf)-1),
        cfg->data_size_pattern,
        cfg->expiry_range.min, cfg->expiry_range.max,
        cfg->data_import,
        cfg->data_verify ? "yes" : "no",
        cfg->verify_only ? "yes" : "no",
        cfg->generate_keys ? "yes" : "no",
        cfg->key_prefix,
        cfg->key_minimum,
        cfg->key_maximum,
        cfg->key_pattern,
        cfg->key_stddev,
        cfg->key_median,
        cfg->reconnect_interval,
        cfg->multi_key_get,
        cfg->authenticate ? cfg->authenticate : "",
        cfg->select_db,
        cfg->no_expiry ? "yes" : "no",
        cfg->wait_ratio.a, cfg->wait_ratio.b,
        cfg->num_slaves.min, cfg->num_slaves.max,
        cfg->wait_timeout.min, cfg->wait_timeout.max,
        cfg->json_out_file);
}

static void config_print_to_json(json_handler * jsonhandler, struct benchmark_config *cfg)
{
    char tmpbuf[512];

    jsonhandler->open_nesting("configuration");

    jsonhandler->write_obj("server"            ,"\"%s\"",      	cfg->server);
    jsonhandler->write_obj("port"              ,"%u",          	cfg->port);
    jsonhandler->write_obj("unix socket"       ,"\"%s\"",      	cfg->unix_socket);
    jsonhandler->write_obj("address family"    ,"\"%s\"",      	cfg->resolution == AF_UNSPEC ? "Unspecified" : cfg->resolution == AF_INET ? "AF_INET" : "AF_INET6");
    jsonhandler->write_obj("protocol"          ,"\"%s\"",      	get_protocol_name(cfg->protocol));
    jsonhandler->write_obj("out_file"          ,"\"%s\"",      	cfg->out_file);
#ifdef USE_TLS
    jsonhandler->write_obj("tls"               ,"\"%s\"",      	cfg->tls ? "true" : "false");
    jsonhandler->write_obj("cert"              ,"\"%s\"",      	cfg->tls_cert);
    jsonhandler->write_obj("key"               ,"\"%s\"",      	cfg->tls_key);
    jsonhandler->write_obj("cacert"            ,"\"%s\"",      	cfg->tls_cacert);
    jsonhandler->write_obj("tls_skip_verify"   ,"\"%s\"",      	cfg->tls_skip_verify ? "true" : "false");
    jsonhandler->write_obj("sni"               ,"\"%s\"",       cfg->tls_sni);
#endif
    jsonhandler->write_obj("client_stats"      ,"\"%s\"",      	cfg->client_stats);
    jsonhandler->write_obj("run_count"         ,"%u",          	cfg->run_count);
    jsonhandler->write_obj("debug"             ,"%u",          	cfg->debug);
    jsonhandler->write_obj("requests"          ,"%llu",        	cfg->requests);
    jsonhandler->write_obj("clients"           ,"%u",          	cfg->clients);
    jsonhandler->write_obj("threads"           ,"%u",          	cfg->threads);
    jsonhandler->write_obj("test_time"         ,"%u",          	cfg->test_time);
    jsonhandler->write_obj("ratio"             ,"\"%u:%u\"",   	cfg->ratio.a, cfg->ratio.b);
    jsonhandler->write_obj("pipeline"          ,"%u",          	cfg->pipeline);
    jsonhandler->write_obj("data_size"         ,"%u",          	cfg->data_size);
    jsonhandler->write_obj("data_offset"       ,"%u",          	cfg->data_offset);
    jsonhandler->write_obj("random_data"       ,"\"%s\"",      	cfg->random_data ? "true" : "false");
    jsonhandler->write_obj("data_size_range"   ,"\"%u:%u\"",	cfg->data_size_range.min, cfg->data_size_range.max);
    jsonhandler->write_obj("data_size_list"    ,"\"%s\"",   	cfg->data_size_list.print(tmpbuf, sizeof(tmpbuf)-1));
    jsonhandler->write_obj("data_size_pattern" ,"\"%s\"", 		cfg->data_size_pattern);
    jsonhandler->write_obj("expiry_range"      ,"\"%u:%u\"",   	cfg->expiry_range.min, cfg->expiry_range.max);
    jsonhandler->write_obj("data_import"       ,"\"%s\"",       cfg->data_import);
    jsonhandler->write_obj("data_verify"       ,"\"%s\"",       cfg->data_verify ? "true" : "false");
    jsonhandler->write_obj("verify_only"       ,"\"%s\"",       cfg->verify_only ? "true" : "false");
    jsonhandler->write_obj("generate_keys"     ,"\"%s\"",     	cfg->generate_keys ? "true" : "false");
    jsonhandler->write_obj("key_prefix"        ,"\"%s\"",       cfg->key_prefix);
    jsonhandler->write_obj("key_minimum"       ,"%11u",        	cfg->key_minimum);
    jsonhandler->write_obj("key_maximum"       ,"%11u",        	cfg->key_maximum);
    jsonhandler->write_obj("key_pattern"       ,"\"%s\"",       cfg->key_pattern);
    jsonhandler->write_obj("key_stddev"        ,"%f",           cfg->key_stddev);
    jsonhandler->write_obj("key_median"        ,"%f",           cfg->key_median);
    jsonhandler->write_obj("reconnect_interval","%u",    		cfg->reconnect_interval);
    jsonhandler->write_obj("multi_key_get"     ,"%u",         	cfg->multi_key_get);
    jsonhandler->write_obj("authenticate"      ,"\"%s\"",      	cfg->authenticate ? cfg->authenticate : "");
    jsonhandler->write_obj("select-db"         ,"%d",           cfg->select_db);
    jsonhandler->write_obj("no-expiry"         ,"\"%s\"",       cfg->no_expiry ? "true" : "false");
    jsonhandler->write_obj("wait-ratio"        ,"\"%u:%u\"",    cfg->wait_ratio.a, cfg->wait_ratio.b);
    jsonhandler->write_obj("num-slaves"        ,"\"%u:%u\"",    cfg->num_slaves.min, cfg->num_slaves.max);
    jsonhandler->write_obj("wait-timeout"      ,"\"%u-%u\"",   	cfg->wait_timeout.min, cfg->wait_timeout.max);

    jsonhandler->close_nesting();
}

static void config_init_defaults(struct benchmark_config *cfg)
{
    if (!cfg->server && !cfg->unix_socket)
        cfg->server = "localhost";
    if (!cfg->port && !cfg->unix_socket)
        cfg->port = 6379;
    if (!cfg->resolution)
        cfg->resolution = AF_UNSPEC;
    if (!cfg->run_count)
        cfg->run_count = 1;
    if (!cfg->clients)
        cfg->clients = 50;
    if (!cfg->threads)
        cfg->threads = 4;
    if (!cfg->ratio.is_defined())
        cfg->ratio = config_ratio("1:10");
    if (!cfg->pipeline)
        cfg->pipeline = 1;
    if (!cfg->data_size && !cfg->data_size_list.is_defined() && !cfg->data_size_range.is_defined() && !cfg->data_import)
        cfg->data_size = 32;
    if (cfg->generate_keys || !cfg->data_import) {
        if (!cfg->key_prefix)
            cfg->key_prefix = "memtier-";
        if (!cfg->key_maximum)
            cfg->key_maximum = 10000000;
    }
    if (!cfg->key_pattern)
        cfg->key_pattern = "R:R";
    if (!cfg->data_size_pattern)
        cfg->data_size_pattern = "R";
    if (cfg->requests == (unsigned long long)-1) {
        cfg->requests = cfg->key_maximum - cfg->key_minimum;
        if (strcmp(cfg->key_pattern, "P:P")==0)
            cfg->requests = cfg->requests / (cfg->clients * cfg->threads) + 1;
        printf("setting requests to %llu\n", cfg->requests);
    }
    if (!cfg->requests && !cfg->test_time)
        cfg->requests = 10000;
    if (!cfg->hdr_prefix)
        cfg->hdr_prefix = "";
    if (!cfg->print_percentiles.is_defined())
        cfg->print_percentiles = config_quantiles("50,99,99.9");
}

static int generate_random_seed()
{
    int R = 0;
    FILE* f = fopen("/dev/random", "r");
    if (f)
    {
        size_t ignore = fread(&R, sizeof(R), 1, f);
        fclose(f);
        ignore++;//ignore warning
    }

    return (int)time(NULL)^getpid()^R;
}

static bool verify_cluster_option(struct benchmark_config *cfg) {
    if (cfg->reconnect_interval) {
        fprintf(stderr, "error: cluster mode dose not support reconnect-interval option.\n");
        return false;
    } else if (cfg->multi_key_get) {
        fprintf(stderr, "error: cluster mode dose not support multi-key-get option.\n");
        return false;
    } else if (cfg->wait_ratio.is_defined()) {
        fprintf(stderr, "error: cluster mode dose not support wait-ratio option.\n");
        return false;
    } else if (!is_redis_protocol(cfg->protocol)) {
        fprintf(stderr, "error: cluster mode supported only in redis protocol.\n");
        return false;
    } else if (cfg->unix_socket) {
        fprintf(stderr, "error: cluster mode dose not support unix-socket option.\n");
        return false;
    } else if (cfg->arbitrary_commands->is_defined()) {
        fprintf(stderr, "error: cluster mode dose not support arbitrary command option.\n");
        return false;
    }

    return true;
}

static bool verify_arbitrary_command_option(struct benchmark_config *cfg) {
    if (cfg->key_pattern) {
        fprintf(stderr, "error: when using arbitrary command, key pattern is configured with --command-key-pattern option.\n");
        return false;
    } else if (cfg->ratio.is_defined()) {
        fprintf(stderr, "error: when using arbitrary command, ratio is configured with --command-ratio option.\n");
        return false;
    }

    // verify that when using Parallel key pattern, it's configured to all commands
    size_t parallel_count = 0;
    for (size_t i = 0; i<cfg->arbitrary_commands->size(); i++) {
        arbitrary_command& cmd =  cfg->arbitrary_commands->at(i);
        if (cmd.key_pattern == 'P') {
            parallel_count++;
        }
    }

    if (parallel_count > 0 && parallel_count != cfg->arbitrary_commands->size()) {
        fprintf(stderr, "error: parallel key-pattern must be configured to all commands.\n");
        return false;
    }

    return true;
}

static int config_parse_args(int argc, char *argv[], struct benchmark_config *cfg)
{
    enum extended_options {
        o_test_time = 128,
        o_ratio,
        o_pipeline,
        o_data_size_range,
        o_data_size_list,
        o_data_size_pattern,
        o_data_offset,
        o_expiry_range,
        o_data_import,
        o_data_verify,
        o_verify_only,
        o_key_prefix,
        o_key_minimum,
        o_key_maximum,
        o_key_pattern,
        o_key_stddev,
        o_key_median,
        o_show_config,
        o_hide_histogram,
        o_print_percentiles,
        o_distinct_client_seed,
        o_randomize,
        o_client_stats,
        o_reconnect_interval,
        o_generate_keys,
        o_multi_key_get,
        o_select_db,
        o_no_expiry,
        o_wait_ratio,
        o_num_slaves,
        o_wait_timeout,
        o_json_out_file,
        o_cluster_mode,
        o_command,
        o_command_key_pattern,
        o_command_ratio,
        o_tls,
        o_tls_cert,
        o_tls_key,
        o_tls_cacert,
        o_tls_skip_verify,
        o_tls_sni,
        o_hdr_file_prefix
    };

    static struct option long_options[] = {
        { "server",                     1, 0, 's' },
        { "port",                       1, 0, 'p' },
        { "unix-socket",                1, 0, 'S' },
        { "ipv4",                       0, 0, '4' },
        { "ipv6",                       0, 0, '6' },
        { "protocol",                   1, 0, 'P' },
#ifdef USE_TLS
        { "tls",                        0, 0, o_tls },
        { "cert",                       1, 0, o_tls_cert },
        { "key",                        1, 0, o_tls_key },
        { "cacert",                     1, 0, o_tls_cacert },
        { "tls-skip-verify",            0, 0, o_tls_skip_verify },
        { "sni",                        1, 0, o_tls_sni },
#endif
        { "out-file",                   1, 0, 'o' },
        { "hdr-file-prefix",            1, 0, o_hdr_file_prefix },
        { "client-stats",               1, 0, o_client_stats },
        { "run-count",                  1, 0, 'x' },
        { "debug",                      0, 0, 'D' },
        { "show-config",                0, 0, o_show_config },
        { "hide-histogram",             0, 0, o_hide_histogram },
        { "print-percentiles",          1, 0, o_print_percentiles },
        { "distinct-client-seed",       0, 0, o_distinct_client_seed },
        { "randomize",                  0, 0, o_randomize },
        { "requests",                   1, 0, 'n' },
        { "clients",                    1, 0, 'c' },
        { "threads",                    1, 0, 't' },
        { "test-time",                  1, 0, o_test_time },
        { "ratio",                      1, 0, o_ratio },
        { "pipeline",                   1, 0, o_pipeline },
        { "data-size",                  1, 0, 'd' },
        { "data-offset",                1, 0, o_data_offset },
        { "random-data",                0, 0, 'R' },
        { "data-size-range",            1, 0, o_data_size_range },
        { "data-size-list",             1, 0, o_data_size_list },
        { "data-size-pattern",          1, 0, o_data_size_pattern },
        { "expiry-range",               1, 0, o_expiry_range },
        { "data-import",                1, 0, o_data_import },
        { "data-verify",                0, 0, o_data_verify },
        { "verify-only",                0, 0, o_verify_only },
        { "generate-keys",              0, 0, o_generate_keys },
        { "key-prefix",                 1, 0, o_key_prefix },
        { "key-minimum",                1, 0, o_key_minimum },
        { "key-maximum",                1, 0, o_key_maximum },
        { "key-pattern",                1, 0, o_key_pattern },
        { "key-stddev",                 1, 0, o_key_stddev },
        { "key-median",                 1, 0, o_key_median },
        { "reconnect-interval",         1, 0, o_reconnect_interval },
        { "multi-key-get",              1, 0, o_multi_key_get },
        { "authenticate",               1, 0, 'a' },
        { "select-db",                  1, 0, o_select_db },
        { "no-expiry",                  0, 0, o_no_expiry },
        { "wait-ratio",                 1, 0, o_wait_ratio },
        { "num-slaves",                 1, 0, o_num_slaves },
        { "wait-timeout",               1, 0, o_wait_timeout },
        { "json-out-file",              1, 0, o_json_out_file },
        { "cluster-mode",               0, 0, o_cluster_mode },
        { "help",                       0, 0, 'h' },
        { "version",                    0, 0, 'v' },
        { "command",                    1, 0, o_command },
        { "command-key-pattern",        1, 0, o_command_key_pattern },
        { "command-ratio",              1, 0, o_command_ratio },
        { NULL,                         0, 0, 0 }
    };

    int option_index;
    int c;
    char *endptr;
    while ((c = getopt_long(argc, argv,
                "vs:S:p:P:o:x:DRn:c:t:d:a:h:46", long_options, &option_index)) != -1)
    {
        switch (c) {
                case 'h':
                    return -1;
                    break;
                case 'v':
                    puts(PACKAGE_STRING);
                    puts("Copyright (C) 2011-2022 Redis Ltd.");
                    puts("This is free software.  You may redistribute copies of it under the terms of");
                    puts("the GNU General Public License <http://www.gnu.org/licenses/gpl.html>.");
                    puts("There is NO WARRANTY, to the extent permitted by law.");
                    exit(0);
                case 's':
                    cfg->server = optarg;
                    break;
                case 'S':
                    cfg->unix_socket = optarg;
                    break;
                case 'p':
                    endptr = NULL;
                    cfg->port = (unsigned short) strtoul(optarg, &endptr, 10);
                    if (!cfg->port || cfg->port > 65535 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: port must be a number in the range [1-65535].\n");
                        return -1;
                    }
                    break;
                case '4':
                    cfg->resolution = AF_INET;
                    break;
                case '6':
                    cfg->resolution = AF_INET6;
                    break;
                case 'P':
                    if (strcmp(optarg, "redis") == 0) {
                        cfg->protocol = PROTOCOL_REDIS_DEFAULT;
                    } else if (strcmp(optarg, "resp2") == 0) {
                        cfg->protocol = PROTOCOL_RESP2;
                    } else if (strcmp(optarg, "resp3") == 0) {
                        cfg->protocol = PROTOCOL_RESP3;
                    } else if (strcmp(optarg, "memcache_text") == 0) {
                        cfg->protocol = PROTOCOL_MEMCACHE_TEXT;
                    } else if (strcmp(optarg, "memcache_binary") == 0) {
                        cfg->protocol = PROTOCOL_MEMCACHE_BINARY;
                    } else {
                        fprintf(stderr, "error: supported protocols are 'memcache_text', 'memcache_binary', 'redis', 'resp2' and resp3'.\n");
                        return -1;
                    }
                    break;
                case 'o':
                    cfg->out_file = optarg;
                    break;
                case o_hdr_file_prefix:
                    cfg->hdr_prefix = optarg;
                    break;
                case o_client_stats:
                    cfg->client_stats = optarg;
                    break;
                case 'x':
                    endptr = NULL;
                    cfg->run_count = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->run_count || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: run count must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 'D':
                    cfg->debug++;
                    break;
                case o_show_config:
                    cfg->show_config++;
                    break;
                case o_hide_histogram:
                    cfg->hide_histogram++;
                    break;
                case o_print_percentiles:
                    cfg->print_percentiles = config_quantiles(optarg);
                    if (!cfg->print_percentiles.is_defined()) {
                        fprintf(stderr, "error: quantiles must be expressed as [0.0-100.0],[0.0-100.0](,...) .\n");
                        return -1;
                    }
                    break;
                case o_distinct_client_seed:
                    cfg->distinct_client_seed++;
                    break;
                case o_randomize:
                    srandom(generate_random_seed());
                    cfg->randomize = random();
                    break;
                case 'n':
                    endptr = NULL;
                    if (strcmp(optarg, "allkeys")==0)
                        cfg->requests = -1;
                    else {
                        cfg->requests = (unsigned long long) strtoull(optarg, &endptr, 10);
                        if (!cfg->requests || !endptr || *endptr != '\0') {
                            fprintf(stderr, "error: requests must be greater than zero.\n");
                            return -1;
                        }
                        if (cfg->test_time) {
                            fprintf(stderr, "error: --test-time and --requests are mutually exclusive.\n");
                            return -1;
                        }
                    }
                    break;
                case 'c':
                    endptr = NULL;
                    cfg->clients = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->clients || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: clients must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 't':
                    endptr = NULL;
                    cfg->threads = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->threads || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: threads must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_test_time:
                    endptr = NULL;
                    cfg->test_time = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->test_time || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: test time must be greater than zero.\n");
                        return -1;
                    }
                    if (cfg->requests) {
                        fprintf(stderr, "error: --test-time and --requests are mutually exclusive.\n");
                        return -1;
                    }
                    break;
                case o_ratio:
                    cfg->ratio = config_ratio(optarg);
                    if (!cfg->ratio.is_defined()) {
                        fprintf(stderr, "error: ratio must be expressed as [0-n]:[0-n].\n");
                        return -1;
                    }
                    break;
                case o_pipeline:
                    endptr = NULL;
                    cfg->pipeline = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->pipeline || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: pipeline must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 'd':
                    endptr = NULL;
                    cfg->data_size = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->data_size || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: data-size must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 'R':
                    cfg->random_data = true;
                    break;
                case o_data_offset:
                    endptr = NULL;
                    cfg->data_offset = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!endptr || *endptr != '\0') {
                        fprintf(stderr, "error: data-offset must be greater than or equal to zero.\n");
                        return -1;
                    }
                    break;
                case o_data_size_range:
                    cfg->data_size_range = config_range(optarg);
                    if (!cfg->data_size_range.is_defined() || cfg->data_size_range.min < 1) {
                        fprintf(stderr, "error: data-size-range must be expressed as [1-n]-[1-n].\n");
                        return -1;
                    }
                    break;
                case o_data_size_list:
                    cfg->data_size_list = config_weight_list(optarg);
                    if (!cfg->data_size_list.is_defined()) {
                        fprintf(stderr, "error: data-size-list must be expressed as [size1:weight1],...[sizeN:weightN].\n");
                        return -1;
                    }
                    break;
                case o_expiry_range:
                    cfg->expiry_range = config_range(optarg);
                    if (!cfg->expiry_range.is_defined()) {
                        fprintf(stderr, "error: expiry-range must be expressed as [0-n]-[1-n].\n");
                        return -1;
                    }
                    break;
                case o_data_size_pattern:
                    cfg->data_size_pattern = optarg;
                    if (strlen(cfg->data_size_pattern) != 1 ||
                        (cfg->data_size_pattern[0] != 'R' && cfg->data_size_pattern[0] != 'S')) {
                            fprintf(stderr, "error: data-size-pattern must be either R or S.\n");
                            return -1;
                    }
                    break;
                case o_data_import:
                    cfg->data_import = optarg;
                    break;
                case o_data_verify:
                    cfg->data_verify = 1;
                    break;
                case o_verify_only:
                    cfg->verify_only = 1;
                    cfg->data_verify = 1;   // Implied
                    break;
                case o_key_prefix:
                    cfg->key_prefix = optarg;
                    break;
                case o_key_minimum:
                    endptr = NULL;
                    cfg->key_minimum = strtoull(optarg, &endptr, 10);
                    if (cfg->key_minimum < 1 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-minimum must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_maximum:
                    endptr = NULL;
                    cfg->key_maximum = strtoull(optarg, &endptr, 10);
                    if (cfg->key_maximum< 1 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-maximum must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_stddev:
                    endptr = NULL;
                    cfg->key_stddev = strtod(optarg, &endptr);
                    if (cfg->key_stddev<= 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-stddev must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_median:
                    endptr = NULL;
                    cfg->key_median = strtod(optarg, &endptr);
                    if (cfg->key_median<= 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-median must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_pattern:
                    cfg->key_pattern = optarg;

                    if (strlen(cfg->key_pattern) != 3 || cfg->key_pattern[key_pattern_delimiter] != ':' ||
                        (cfg->key_pattern[key_pattern_set] != 'R' &&
                         cfg->key_pattern[key_pattern_set] != 'S' &&
                         cfg->key_pattern[key_pattern_set] != 'G' &&
                         cfg->key_pattern[key_pattern_set] != 'P') ||
                        (cfg->key_pattern[key_pattern_get] != 'R' &&
                         cfg->key_pattern[key_pattern_get] != 'S' &&
                         cfg->key_pattern[key_pattern_get] != 'G' &&
                         cfg->key_pattern[key_pattern_get] != 'P')) {
                        fprintf(stderr, "error: key-pattern must be in the format of [S/R/G/P]:[S/R/G/P].\n");
                        return -1;
                    }

                    if ((cfg->key_pattern[key_pattern_set] == 'P' ||
                         cfg->key_pattern[key_pattern_get] == 'P') &&
                        (cfg->key_pattern[key_pattern_set] != cfg->key_pattern[key_pattern_get])) {

                        fprintf(stderr, "error: parallel key-pattern must be configured for both SET and GET commands.\n");
                        return -1;
                    }
                    break;
                case o_reconnect_interval:
                    endptr = NULL;
                    cfg->reconnect_interval = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->reconnect_interval || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: reconnect-interval must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_generate_keys:
                    cfg->generate_keys = 1;
                    break;
                case o_multi_key_get:
                    endptr = NULL;
                    cfg->multi_key_get = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (cfg->multi_key_get <= 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: multi-key-get must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 'a':
                    cfg->authenticate = optarg;
                    break;
                case o_select_db:
                    cfg->select_db = (int) strtoul(optarg, &endptr, 10);
                    if (cfg->select_db < 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: select-db must be greater or equal zero.\n");
                        return -1;
                    }
                    break;
                case o_no_expiry:
                    cfg->no_expiry = true;
                    break;
                case o_wait_ratio:
                    cfg->wait_ratio = config_ratio(optarg);
                    if (!cfg->wait_ratio.is_defined()) {
                        fprintf(stderr, "error: wait-ratio must be expressed as [0-n]:[0-n].\n");
                        return -1;
                    }
                    break;
                case o_num_slaves:
                    cfg->num_slaves = config_range(optarg);
                    if (!cfg->num_slaves.is_defined()) {
                        fprintf(stderr, "error: num-slaves must be expressed as [0-n]-[1-n].\n");
                        return -1;
                    }
                    break;
                case o_wait_timeout:
                    cfg->wait_timeout = config_range(optarg);
                    if (!cfg->wait_timeout.is_defined()) {
                        fprintf(stderr, "error: wait-timeout must be expressed as [0-n]-[1-n].\n");
                        return -1;
                    }
                    break;
                case o_json_out_file:
                    cfg->json_out_file = optarg;
                    break;
                case o_cluster_mode:
                    cfg->cluster_mode = true;
                    break;
                case o_command: {
                    // add new arbitrary command
                    arbitrary_command cmd(optarg);

                    if (cmd.split_command_to_args()) {
                        cfg->arbitrary_commands->add_command(cmd);
                    } else {
                        fprintf(stderr, "error: failed to parse arbitrary command.\n");
                        return -1;
                    }
                    break;
                }
                case o_command_key_pattern: {
                    if (cfg->arbitrary_commands->size() == 0) {
                        fprintf(stderr, "error: no arbitrary command found.\n");
                        return -1;
                    }

                    // command configuration always applied on last configured command
                    arbitrary_command& cmd = cfg->arbitrary_commands->get_last_command();
                    if (!cmd.set_key_pattern(optarg)) {
                        fprintf(stderr, "error: key-pattern for command %s must be in the format of [S/R/G/P].\n", cmd.command_name.c_str());
                        return -1;
                    }
                    break;
                }
                case o_command_ratio: {
                    if (cfg->arbitrary_commands->size() == 0) {
                        fprintf(stderr, "error: no arbitrary command found.\n");
                        return -1;
                    }

                    // command configuration always applied on last configured command
                    arbitrary_command& cmd = cfg->arbitrary_commands->get_last_command();
                    if (!cmd.set_ratio(optarg)) {
                        fprintf(stderr, "error: failed to set ratio for command %s.\n", cmd.command_name.c_str());
                        return -1;
                    }
                    break;
                }
#ifdef USE_TLS
                case o_tls:
                    cfg->tls = true;
                    break;
                case o_tls_cert:
                    cfg->tls_cert = optarg;
                    break;
                case o_tls_key:
                    cfg->tls_key = optarg;
                    break;
                case o_tls_cacert:
                    cfg->tls_cacert = optarg;
                    break;
                case o_tls_skip_verify:
                    cfg->tls_skip_verify = true;
                    break;
                case o_tls_sni:
                    cfg->tls_sni = optarg;
                    break;
#endif
            default:
                    return -1;
                    break;
        }
    }

    if ((cfg->cluster_mode && !verify_cluster_option(cfg)) ||
        (cfg->arbitrary_commands->is_defined() && !verify_arbitrary_command_option(cfg))) {
        return -1;
    }

    return 0;
}

void usage() {
    fprintf(stdout, "Usage: memtier_benchmark [options]\n"
            "A memcache/redis NoSQL traffic generator and performance benchmarking tool.\n"
            "\n"
            "Connection and General Options:\n"
            "  -s, --server=ADDR              Server address (default: localhost)\n"
            "  -p, --port=PORT                Server port (default: 6379)\n"
            "  -S, --unix-socket=SOCKET       UNIX Domain socket name (default: none)\n"
            "  -4, --ipv4                     Force IPv4 address resolution.\n"
            "  -6  --ipv6                     Force IPv6 address resolution.\n"
            "  -P, --protocol=PROTOCOL        Protocol to use (default: redis).\n"
            "                                 other supported protocols are resp2, resp3, memcache_text and memcache_binary.\n"
            "                                 when using one of resp2 or resp3 the redis protocol version will be set via HELLO command.\n"
            "  -a, --authenticate=CREDENTIALS Authenticate using specified credentials.\n"
            "                                 A simple password is used for memcache_text\n"
            "                                 and Redis <= 5.x. <USER>:<PASSWORD> can be\n"
            "                                 specified for memcache_binary or Redis 6.x\n"
            "                                 or newer with ACL user support.\n"
#ifdef USE_TLS
            "      --tls                      Enable SSL/TLS transport security\n"
            "      --cert=FILE                Use specified client certificate for TLS\n"
            "      --key=FILE                 Use specified private key for TLS\n"
            "      --cacert=FILE              Use specified CA certs bundle for TLS\n"
            "      --tls-skip-verify          Skip verification of server certificate\n"
            "      --sni=STRING               Add an SNI header\n"
#endif
            "  -x, --run-count=NUMBER         Number of full-test iterations to perform\n"
            "  -D, --debug                    Print debug output\n"
            "      --client-stats=FILE        Produce per-client stats file\n"
            "  -o, --out-file=FILE            Name of output file (default: stdout)\n"
            "      --json-out-file=FILE       Name of JSON output file, if not set, will not print to json\n"
            "      --hdr-file-prefix=FILE     Prefix of HDR Latency Histogram output files, if not set, will not save latency histogram files\n"
            "      --show-config              Print detailed configuration before running\n"
            "      --hide-histogram           Don't print detailed latency histogram\n"
            "      --print-percentiles        Specify which percentiles info to print on the results table (by default prints percentiles: 50,99,99.9)\n"
            "      --cluster-mode             Run client in cluster mode\n"
            "  -h, --help                     Display this help\n"
            "  -v, --version                  Display version information\n"
            "\n"
            "Test Options:\n"
            "  -n, --requests=NUMBER          Number of total requests per client (default: 10000)\n"
            "                                 use 'allkeys' to run on the entire key-range\n"
            "  -c, --clients=NUMBER           Number of clients per thread (default: 50)\n"
            "  -t, --threads=NUMBER           Number of threads (default: 4)\n"
            "      --test-time=SECS           Number of seconds to run the test\n"
            "      --ratio=RATIO              Set:Get ratio (default: 1:10)\n"
            "      --pipeline=NUMBER          Number of concurrent pipelined requests (default: 1)\n"
            "      --reconnect-interval=NUM   Number of requests after which re-connection is performed\n"
            "      --multi-key-get=NUM        Enable multi-key get commands, up to NUM keys (default: 0)\n"
            "      --select-db=DB             DB number to select, when testing a redis server\n"
            "      --distinct-client-seed     Use a different random seed for each client\n"
            "      --randomize                random seed based on timestamp (default is constant value)\n"
            "\n"
            "Arbitrary command:\n"
            "      --command=COMMAND          Specify a command to send in quotes.\n"
            "                                 Each command that you specify is run with its ratio and key-pattern options.\n"
            "                                 For example: --command=\"set __key__ 5\" --command-ratio=2 --command-key-pattern=G\n"
            "                                 To use a generated key or object, enter:\n"
            "                                   __key__: Use key generated from Key Options.\n"
            "                                   __data__: Use data generated from Object Options.\n"
            "      --command-ratio            The number of times the command is sent in sequence.(default: 1)\n"
            "      --command-key-pattern      Key pattern for the command (default: R):\n"
            "                                 G for Gaussian distribution.\n"
            "                                 R for uniform Random.\n"
            "                                 S for Sequential.\n"
            "                                 P for Parallel (Sequential were each client has a subset of the key-range).\n"
            "\n"
            "Object Options:\n"
            "  -d  --data-size=SIZE           Object data size in bytes (default: 32)\n"
            "      --data-offset=OFFSET       Actual size of value will be data-size + data-offset\n"
            "                                 Will use SETRANGE / GETRANGE (default: 0)\n"
            "  -R  --random-data              Indicate that data should be randomized\n"
            "      --data-size-range=RANGE    Use random-sized items in the specified range (min-max)\n"
            "      --data-size-list=LIST      Use sizes from weight list (size1:weight1,..sizeN:weightN)\n"
            "      --data-size-pattern=R|S    Use together with data-size-range\n"
            "                                 when set to R, a random size from the defined data sizes will be used,\n"
            "                                 when set to S, the defined data sizes will be evenly distributed across\n"
            "                                 the key range, see --key-maximum (default R)\n"
            "      --expiry-range=RANGE       Use random expiry values from the specified range\n"
            "\n"
            "Imported Data Options:\n"
            "      --data-import=FILE         Read object data from file\n"
            "      --data-verify              Enable data verification when test is complete\n"
            "      --verify-only              Only perform --data-verify, without any other test\n"
            "      --generate-keys            Generate keys for imported objects\n"
            "      --no-expiry                Ignore expiry information in imported data\n"
            "\n"
            "Key Options:\n"
            "      --key-prefix=PREFIX        Prefix for keys (default: \"memtier-\")\n"
            "      --key-minimum=NUMBER       Key ID minimum value (default: 0)\n"
            "      --key-maximum=NUMBER       Key ID maximum value (default: 10000000)\n"
            "      --key-pattern=PATTERN      Set:Get pattern (default: R:R)\n"
            "                                 G for Gaussian distribution.\n"
            "                                 R for uniform Random.\n"
            "                                 S for Sequential.\n"
            "                                 P for Parallel (Sequential were each client has a subset of the key-range).\n"
            "      --key-stddev               The standard deviation used in the Gaussian distribution\n"
            "                                 (default is key range / 6)\n"
            "      --key-median               The median point used in the Gaussian distribution\n"
            "                                 (default is the center of the key range)\n"
            "\n"
            "WAIT Options:\n"
            "      --wait-ratio=RATIO         Set:Wait ratio (default is no WAIT commands - 1:0)\n"
            "      --num-slaves=RANGE         WAIT for a random number of slaves in the specified range\n"
            "      --wait-timeout=RANGE       WAIT for a random number of milliseconds in the specified range (normal \n"
            "                                 distribution with the center in the middle of the range)"
            "\n"
            );

    exit(2);
}

static void* cg_thread_start(void *t);

struct cg_thread {
    unsigned int m_thread_id;
    benchmark_config* m_config;
    object_generator* m_obj_gen;
    client_group* m_cg;
    abstract_protocol* m_protocol;
    pthread_t m_thread;
    bool m_finished;

    cg_thread(unsigned int id, benchmark_config* config, object_generator* obj_gen) :
        m_thread_id(id), m_config(config), m_obj_gen(obj_gen), m_cg(NULL), m_protocol(NULL), m_finished(false)
    {
        m_protocol = protocol_factory(m_config->protocol);
        assert(m_protocol != NULL);

        m_cg = new client_group(m_config, m_protocol, m_obj_gen);
    }

    ~cg_thread()
    {
        if (m_cg != NULL) {
            delete m_cg;
        }
        if (m_protocol != NULL) {
            delete m_protocol;
        }
    }

    int prepare(void)
    {
        if (m_cg->create_clients(m_config->clients) < (int) m_config->clients)
            return -1;
        return m_cg->prepare();
    }

    int start(void)
    {
        return pthread_create(&m_thread, NULL, cg_thread_start, (void *)this);
    }

    void join(void)
    {
        int* retval;
        int ret;

        ret = pthread_join(m_thread, (void **)&retval);
        assert(ret == 0);
    }

};

static void* cg_thread_start(void *t)
{
    cg_thread* thread = (cg_thread*) t;
    thread->m_cg->run();
    thread->m_finished = true;

    return t;
}

void size_to_str(unsigned long int size, char *buf, int buf_len)
{
    if (size >= 1024*1024*1024) {
        snprintf(buf, buf_len, "%.2fGB",
            (float) size / (1024*1024*1024));
    } else if (size >= 1024*1024) {
        snprintf(buf, buf_len, "%.2fMB",
            (float) size / (1024*1024));
    } else {
        snprintf(buf, buf_len, "%.2fKB",
            (float) size / 1024);
    }
}

run_stats run_benchmark(int run_id, benchmark_config* cfg, object_generator* obj_gen)
{
    fprintf(stderr, "[RUN #%u] Preparing benchmark client...\n", run_id);

    // prepare threads data
    std::vector<cg_thread*> threads;
    for (unsigned int i = 0; i < cfg->threads; i++) {
        cg_thread* t = new cg_thread(i, cfg, obj_gen);
        assert(t != NULL);

        if (t->prepare() < 0) {
            benchmark_error_log("error: failed to prepare thread %u for test.\n", i);
            exit(1);
        }
        threads.push_back(t);
    }

    // launch threads
    fprintf(stderr, "[RUN #%u] Launching threads now...\n", run_id);
    for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
        (*i)->start();
    }

    unsigned long int prev_ops = 0;
    unsigned long int prev_bytes = 0;
    unsigned long int prev_duration = 0;
    double prev_latency = 0, cur_latency = 0;
    unsigned long int cur_ops_sec = 0;
    unsigned long int cur_bytes_sec = 0;

    // provide some feedback...
    unsigned int active_threads = 0;
    do {
        active_threads = 0;
        sleep(1);

        unsigned long int total_ops = 0;
        unsigned long int total_bytes = 0;
        unsigned long int duration = 0;
        unsigned int thread_counter = 0;
        unsigned long int total_latency = 0;

        for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
            if (!(*i)->m_finished)
                active_threads++;

            total_ops += (*i)->m_cg->get_total_ops();
            total_bytes += (*i)->m_cg->get_total_bytes();
            total_latency += (*i)->m_cg->get_total_latency();
            thread_counter++;
            float factor = ((float)(thread_counter - 1) / thread_counter);
            duration =  factor * duration +  (float)(*i)->m_cg->get_duration_usec() / thread_counter ;
        }

        unsigned long int cur_ops = total_ops-prev_ops;
        unsigned long int cur_bytes = total_bytes-prev_bytes;
        unsigned long int cur_duration = duration-prev_duration;
        double cur_total_latency = total_latency-prev_latency;
        prev_ops = total_ops;
        prev_bytes = total_bytes;
        prev_latency = total_latency;
        prev_duration = duration;

        unsigned long int ops_sec = 0;
        unsigned long int bytes_sec = 0;
        double avg_latency = 0;
        if (duration > 1) {
            ops_sec = (long)( (double)total_ops / duration * 1000000);
            bytes_sec = (long)( (double)total_bytes / duration * 1000000);
            avg_latency = ((double) total_latency / 1000 / total_ops) ;
        }
        if (cur_duration > 1 && active_threads == cfg->threads) {
            cur_ops_sec = (long)( (double)cur_ops / cur_duration * 1000000);
            cur_bytes_sec = (long)( (double)cur_bytes / cur_duration * 1000000);
            cur_latency = ((double) cur_total_latency / 1000 / cur_ops) ;
        }

        char bytes_str[40], cur_bytes_str[40];
        size_to_str(bytes_sec, bytes_str, sizeof(bytes_str)-1);
        size_to_str(cur_bytes_sec, cur_bytes_str, sizeof(cur_bytes_str)-1);

        double progress = 0;
        if(cfg->requests)
            progress = 100.0 * total_ops / ((double)cfg->requests*cfg->clients*cfg->threads);
        else
            progress = 100.0 * (duration / 1000000.0)/cfg->test_time;

        fprintf(stderr, "[RUN #%u %.0f%%, %3u secs] %2u threads: %11lu ops, %7lu (avg: %7lu) ops/sec, %s/sec (avg: %s/sec), %5.2f (avg: %5.2f) msec latency\r",
            run_id, progress, (unsigned int) (duration / 1000000), active_threads, total_ops, cur_ops_sec, ops_sec, cur_bytes_str, bytes_str, cur_latency, avg_latency);
    } while (active_threads > 0);

    fprintf(stderr, "\n\n");

    // join all threads back and unify stats
    run_stats stats(cfg);

    for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
        (*i)->join();
        (*i)->m_cg->merge_run_stats(&stats);
    }

    // Do we need to produce client stats?
    if (cfg->client_stats != NULL) {
        unsigned int cg_id = 0;
        fprintf(stderr, "[RUN %u] Writing client stats files...\n", run_id);
        for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
            char prefix[PATH_MAX];

            snprintf(prefix, sizeof(prefix)-1, "%s-%u-%u", cfg->client_stats, run_id, cg_id++);
            (*i)->m_cg->write_client_stats(prefix);
        }
    }

    // clean up all client_groups.  the main value of this is to be able to
    // properly look for leaks...
    while (threads.size() > 0) {
        cg_thread* t = *threads.begin();
        threads.erase(threads.begin());
        delete t;
    }

    return stats;
}

#ifdef USE_TLS

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
static pthread_mutex_t *__openssl_locks;

static void __openssl_locking_callback(int mode, int type, const char *file, int line)
{
    if (mode & CRYPTO_LOCK) {
        pthread_mutex_lock(&(__openssl_locks[type]));
    } else {
        pthread_mutex_unlock(&(__openssl_locks[type]));
    }
}

static unsigned long __openssl_thread_id(void)
{
    unsigned long id;

    id = (unsigned long) pthread_self();
    return id;
}
#pragma GCC diagnostic pop

static void init_openssl_threads(void)
{
    int i;

    __openssl_locks = (pthread_mutex_t *) malloc(CRYPTO_num_locks() * sizeof(pthread_mutex_t));
    assert(__openssl_locks != NULL);

    for (i = 0; i < CRYPTO_num_locks(); i++) {
        pthread_mutex_init(&(__openssl_locks[i]), NULL);
    }

    CRYPTO_set_id_callback(__openssl_thread_id);
    CRYPTO_set_locking_callback(__openssl_locking_callback);
}

static void cleanup_openssl_threads(void)
{
    int i;

    CRYPTO_set_locking_callback(NULL);
    for (i = 0; i < CRYPTO_num_locks(); i++) {
        pthread_mutex_destroy(&(__openssl_locks[i]));
    }
    OPENSSL_free(__openssl_locks);
}

static void init_openssl(void)
{
    SSL_library_init();
    SSL_load_error_strings();
    if (!RAND_poll()) {
        fprintf(stderr, "Failed to initialize OpenSSL random entropy.\n");
        exit(1);
    }

    //Enable memtier benchmark to load an OpenSSL config file.
    #if OPENSSL_VERSION_NUMBER < 0x10100000L
    OPENSSL_config(NULL);
    #else
    OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL);
    #endif


    init_openssl_threads();
}

static void cleanup_openssl(void)
{
    cleanup_openssl_threads();
}

#endif

int main(int argc, char *argv[])
{
    benchmark_config cfg = benchmark_config();
    cfg.arbitrary_commands = new arbitrary_command_list();

    if (config_parse_args(argc, argv, &cfg) < 0) {
        usage();
    }

    config_init_defaults(&cfg);
    log_level = cfg.debug;
    if (cfg.show_config) {
        fprintf(stderr, "============== Configuration values: ==============\n");
        config_print(stdout, &cfg);
        fprintf(stderr, "===================================================\n");
    }

    // if user configure arbitrary commands, format and prepare it
    for (unsigned int i=0; i<cfg.arbitrary_commands->size(); i++) {
        abstract_protocol* tmp_protocol = protocol_factory(cfg.protocol);
        assert(tmp_protocol != NULL);

        if (!tmp_protocol->format_arbitrary_command(cfg.arbitrary_commands->at(i))) {
            exit(1);
        }

        delete tmp_protocol;
    }


#ifdef USE_TLS
    // Initialize OpenSSL only if we're really going to use it.
    if (cfg.tls) {
        init_openssl();

        cfg.openssl_ctx = SSL_CTX_new(SSLv23_client_method());
        SSL_CTX_set_options(cfg.openssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

        if (cfg.tls_cert) {
            if (!SSL_CTX_use_certificate_chain_file(cfg.openssl_ctx, cfg.tls_cert)) {
                ERR_print_errors_fp(stderr);
                fprintf(stderr, "Error: Failed to load certificate file.\n");
                exit(1);
            }
            if (!SSL_CTX_use_PrivateKey_file(cfg.openssl_ctx,
                        cfg.tls_key ? cfg.tls_key : cfg.tls_cert,
                        SSL_FILETYPE_PEM)) {
                ERR_print_errors_fp(stderr);
                fprintf(stderr, "Error: Failed to load private key file.\n");
                exit(1);
            }
        }
        if (cfg.tls_cacert) {
            if (!SSL_CTX_load_verify_locations(cfg.openssl_ctx, cfg.tls_cacert,
                        NULL)) {
                ERR_print_errors_fp(stderr);
                fprintf(stderr, "Error: Failed to load CA certificate file.\n");
                exit(1);
            }
        }
        SSL_CTX_set_verify(cfg.openssl_ctx,
                cfg.tls_skip_verify ? SSL_VERIFY_NONE : SSL_VERIFY_PEER,
                NULL);
    }
#endif

    // JSON file initiation
    json_handler *jsonhandler = NULL;
    if (cfg.json_out_file != NULL){
        jsonhandler = new json_handler((const char *)cfg.json_out_file);
        // We allways print the configuration to the JSON file
        config_print_to_json(jsonhandler,&cfg);
    }

    struct rlimit rlim;
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        benchmark_error_log("error: getrlimit failed: %s\n", strerror(errno));
        exit(1);
    }

    if (cfg.unix_socket != NULL &&
        (cfg.server != NULL || cfg.port > 0)) {
        benchmark_error_log("error: UNIX domain socket and TCP cannot be used together.\n");
        exit(1);
    }

    if (cfg.server != NULL && cfg.port > 0) {
        try {
            cfg.server_addr = new server_addr(cfg.server, cfg.port, cfg.resolution);
        } catch (std::runtime_error& e) {
            benchmark_error_log("%s:%u: error: %s\n",
                    cfg.server, cfg.port, e.what());
            exit(1);
        }
    }

    unsigned int fds_needed = (cfg.threads * cfg.clients) + (cfg.threads * 10) + 10;
    if (fds_needed > rlim.rlim_cur) {
        if (fds_needed > rlim.rlim_max && getuid() != 0) {
            benchmark_error_log("error: running the tool with this number of connections requires 'root' privilegs.\n");
            exit(1);
        }
        rlim.rlim_cur = rlim.rlim_max = fds_needed;

        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            benchmark_error_log("error: setrlimit failed: %s\n", strerror(errno));
            exit(1);
        }
    }

    // create and configure object generator
    object_generator* obj_gen = NULL;
    imported_keylist* keylist = NULL;
    if (!cfg.data_import) {
        if (cfg.data_verify) {
            fprintf(stderr, "error: use data-verify only with data-import\n");
            exit(1);
        }
        if (cfg.no_expiry) {
            fprintf(stderr, "error: use no-expiry only with data-import\n");
            exit(1);
        }

        if (cfg.arbitrary_commands->is_defined()) {
            obj_gen = new object_generator(cfg.arbitrary_commands->size());
        } else {
            obj_gen = new object_generator();
        }
        assert(obj_gen != NULL);
    } else {
        // check paramters
        if (cfg.data_size ||
            cfg.data_size_list.is_defined() ||
            cfg.data_size_range.is_defined()) {
            fprintf(stderr, "error: data size cannot be specified when importing.\n");
            exit(1);
        }

        if (cfg.random_data) {
            fprintf(stderr, "error: random-data cannot be specified when importing.\n");
            exit(1);
        }

        if (!cfg.generate_keys &&
            (cfg.key_maximum || cfg.key_minimum || cfg.key_prefix)) {
                fprintf(stderr, "error: use key-minimum, key-maximum and key-prefix only with generate-keys.\n");
                exit(1);
        }

        if (!cfg.generate_keys) {
            // read keys
            fprintf(stderr, "Reading keys from %s...", cfg.data_import);
            keylist = new imported_keylist(cfg.data_import);
            assert(keylist != NULL);

            if (!keylist->read_keys()) {
                fprintf(stderr, "\nerror: failed to read keys.\n");
                exit(1);
            } else {
                fprintf(stderr, " %u keys read.\n", keylist->size());
            }
        }

        obj_gen = new import_object_generator(cfg.data_import, keylist, cfg.no_expiry);
        assert(obj_gen != NULL);

        if (dynamic_cast<import_object_generator*>(obj_gen)->open_file() != true) {
            fprintf(stderr, "error: %s: failed to open.\n", cfg.data_import);
            exit(1);
        }
    }

    if (cfg.authenticate) {
        if (cfg.protocol == PROTOCOL_MEMCACHE_TEXT) {
                fprintf(stderr, "error: authenticate can only be used with redis or memcache_binary.\n");
                usage();
        }
        if (cfg.protocol == PROTOCOL_MEMCACHE_BINARY &&
            strchr(cfg.authenticate, ':') == NULL) {
                fprintf(stderr, "error: binary_memcache credentials must be in the form of USER:PASSWORD.\n");
                usage();
        }
    }
    if (!cfg.data_import) {
        obj_gen->set_random_data(cfg.random_data);
    }

    if (cfg.select_db > 0 && !is_redis_protocol(cfg.protocol)) {
        fprintf(stderr, "error: select-db can only be used with redis protocol.\n");
        usage();
    }
    if (cfg.data_offset > 0) {
        if (cfg.data_offset > (1<<29)-1) {
            fprintf(stderr, "error: data-offset too long\n");
            usage();
        }
        if (cfg.expiry_range.min || cfg.expiry_range.max || !is_redis_protocol(cfg.protocol)) {
            fprintf(stderr, "error: data-offset can only be used with redis protocol, and cannot be used with expiry\n");
            usage();
        }
    }
    if (cfg.data_size) {
        if (cfg.data_size_list.is_defined() || cfg.data_size_range.is_defined()) {
            fprintf(stderr, "error: data-size cannot be used with data-size-list or data-size-range.\n");
            usage();
        }
        obj_gen->set_data_size_fixed(cfg.data_size);
    } else if (cfg.data_size_list.is_defined()) {
        if (cfg.data_size_range.is_defined()) {
            fprintf(stderr, "error: data-size-list cannot be used with data-size-range.\n");
            usage();
        }
        obj_gen->set_data_size_list(&cfg.data_size_list);
    } else if (cfg.data_size_range.is_defined()) {
        obj_gen->set_data_size_range(cfg.data_size_range.min, cfg.data_size_range.max);
        obj_gen->set_data_size_pattern(cfg.data_size_pattern);
    } else if (!cfg.data_import) {
        fprintf(stderr, "error: data-size, data-size-list or data-size-range must be specified.\n");
        usage();
    }

    if (!cfg.data_import || cfg.generate_keys) {
        obj_gen->set_key_prefix(cfg.key_prefix);
        obj_gen->set_key_range(cfg.key_minimum, cfg.key_maximum);
    }
    if (cfg.key_stddev>0 || cfg.key_median>0) {
        if (cfg.key_pattern[key_pattern_set]!='G' && cfg.key_pattern[key_pattern_get]!='G') {
            fprintf(stderr, "error: key-stddev and key-median are only allowed together with key-pattern set to G.\n");
            usage();
        }
        if (cfg.key_median!=0 && (cfg.key_median<cfg.key_minimum || cfg.key_median>cfg.key_maximum)) {
            fprintf(stderr, "error: key-median must be between key-minimum and key-maximum.\n");
            usage();
        }
        obj_gen->set_key_distribution(cfg.key_stddev, cfg.key_median);
    }
    obj_gen->set_expiry_range(cfg.expiry_range.min, cfg.expiry_range.max);

    // Prepare output file
    FILE *outfile;
    if (cfg.out_file != NULL) {
        fprintf(stderr, "Writing results to %s...\n", cfg.out_file);
        outfile = fopen(cfg.out_file, "w");
        if (!outfile) {
            perror(cfg.out_file);
        }
    } else {
        fprintf(stderr, "Writing results to stdout\n");
        outfile = stdout;
    }

    if (!cfg.verify_only) {
        std::vector<run_stats> all_stats;
        all_stats.reserve(cfg.run_count);

        for (unsigned int run_id = 1; run_id <= cfg.run_count; run_id++) {
            if (run_id > 1)
                sleep(1);   // let connections settle

            run_stats stats = run_benchmark(run_id, &cfg, obj_gen);
            all_stats.push_back(stats);
            stats.save_hdr_full_run( &cfg,run_id );
            stats.save_hdr_get_command( &cfg,run_id );
            stats.save_hdr_set_command( &cfg,run_id );
            stats.save_hdr_arbitrary_commands( &cfg,run_id );
        }
        //
        // Print some run information
        fprintf(outfile,
               "%-9u Threads\n"
               "%-9u Connections per thread\n"
               "%-9llu %s\n",
               cfg.threads, cfg.clients,
               (unsigned long long)(cfg.requests > 0 ? cfg.requests : cfg.test_time),
               cfg.requests > 0 ? "Requests per client"  : "Seconds");

        if (jsonhandler != NULL){
            jsonhandler->open_nesting("run information");
            jsonhandler->write_obj("Threads","%u",cfg.threads);
            jsonhandler->write_obj("Connections per thread","%u",cfg.clients);
            jsonhandler->write_obj(cfg.requests > 0 ? "Requests per client"  : "Seconds","%llu",
                                   cfg.requests > 0 ? cfg.requests : (unsigned long long)cfg.test_time);
            jsonhandler->write_obj("Format version","%d",2);
            jsonhandler->close_nesting();
        }

        // If more than 1 run was used, compute best, worst and average
        if (cfg.run_count > 1) {
            unsigned int min_ops_sec = (unsigned int) -1;
            unsigned int max_ops_sec = 0;
            run_stats* worst = NULL;
            run_stats* best = NULL;
            for (std::vector<run_stats>::iterator i = all_stats.begin(); i != all_stats.end(); i++) {
                unsigned long usecs = i->get_duration_usec();
                unsigned int ops_sec = (int)(((double)i->get_total_ops() / (usecs > 0 ? usecs : 1)) * 1000000);
                if (ops_sec < min_ops_sec || worst == NULL) {
                    min_ops_sec = ops_sec;
                    worst = &(*i);
                }
                if (ops_sec > max_ops_sec || best == NULL) {
                    max_ops_sec = ops_sec;
                    best = &(*i);
                }
            }

            // Best results:
            best->print(outfile, &cfg, "BEST RUN RESULTS", jsonhandler);
            // worst results:
            worst->print(outfile, &cfg, "WORST RUN RESULTS", jsonhandler);
            // average results:
            run_stats average(&cfg);
            average.aggregate_average(all_stats);
            char average_header[50];
            sprintf(average_header,"AGGREGATED AVERAGE RESULTS (%u runs)", cfg.run_count);
            average.print(outfile, &cfg, average_header, jsonhandler);
        } else {
            all_stats.begin()->print(outfile, &cfg, "ALL STATS", jsonhandler);
        }
    }

    // If needed, data verification is done now...
    if (cfg.data_verify) {
        struct event_base *verify_event_base = event_base_new();
        abstract_protocol *verify_protocol = protocol_factory(cfg.protocol);
        verify_client *client = new verify_client(verify_event_base, &cfg, verify_protocol, obj_gen);

        fprintf(outfile, "\n\nPerforming data verification...\n");

        // Run client in verification mode
        client->prepare();
        event_base_dispatch(verify_event_base);

        fprintf(outfile, "Data verification completed:\n"
                        "%-10llu keys verified successfuly.\n"
                        "%-10llu keys failed.\n",
                        client->get_verified_keys(),
                        client->get_errors());

        if (jsonhandler != NULL){
            jsonhandler->open_nesting("client verifications results");
            jsonhandler->write_obj("keys verified successfuly", "%-10llu",  client->get_verified_keys());
            jsonhandler->write_obj("keys failed", "%-10llu",  client->get_errors());
            jsonhandler->close_nesting();
        }

        // Clean up...
        delete client;
        delete verify_protocol;
        event_base_free(verify_event_base);
    }

    if (outfile != stdout) {
        fclose(outfile);
    }

    if (cfg.server_addr) {
        delete cfg.server_addr;
        cfg.server_addr = NULL;
    }

    if (jsonhandler != NULL) {
        // closing the JSON
        delete jsonhandler;
    }

    delete obj_gen;
    if (keylist != NULL)
        delete keylist;

    if (cfg.arbitrary_commands != NULL) {
        delete cfg.arbitrary_commands;
    }

#ifdef USE_TLS
    if(cfg.tls) {
        if (cfg.openssl_ctx) {
            SSL_CTX_free(cfg.openssl_ctx);
            cfg.openssl_ctx = NULL;
        }

        cleanup_openssl();
    }
#endif
}
