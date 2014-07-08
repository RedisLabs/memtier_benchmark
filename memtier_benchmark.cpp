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

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#ifdef HAVE_PTHREAD_H
#include <pthread.h>
#endif

#include "client.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"

#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>

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


static void config_print(FILE *file, struct benchmark_config *cfg)
{
    char tmpbuf[512];
    
    fprintf(file,
        "server = %s\n"
        "port = %u\n"
        "unix socket = %s\n"
        "protocol = %s\n"
        "out_file = %s\n"
        "client_stats = %s\n"
        "run_count = %u\n"
        "debug = %u\n"
        "requests = %u\n"
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
        "key_minimum = %u\n"
        "key_maximum = %u\n"
        "key_pattern = %s\n"
        "key_stddev = %f\n"
        "key_median = %f\n"
        "reconnect_interval = %u\n"
        "multi_key_get = %u\n"
        "authenticate = %s\n"
        "select-db = %d\n"
        "no-expiry = %s\n",
        cfg->server,
        cfg->port,
        cfg->unix_socket,
        cfg->protocol,
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
        cfg->no_expiry ? "yes" : "no");
}

static void config_init_defaults(struct benchmark_config *cfg)
{
    if (!cfg->server && !cfg->unix_socket)
        cfg->server = "localhost";
    if (!cfg->port && !cfg->unix_socket)
        cfg->port = 6379;
    if (!cfg->protocol)
        cfg->protocol = "redis";
    if (!cfg->run_count)
        cfg->run_count = 1;
    if (!cfg->requests && !cfg->test_time)
        cfg->requests = 10000;
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
        o_client_stats,
        o_reconnect_interval,
        o_generate_keys,
        o_multi_key_get,
        o_select_db,
        o_no_expiry
    };
    
    static struct option long_options[] = {
        { "server",                     1, 0, 's' },
        { "port",                       1, 0, 'p' },
        { "unix-socket",                1, 0, 'S' },
        { "protocol",                   1, 0, 'P' },
        { "out-file",                   1, 0, 'o' },
        { "client-stats",               1, 0, o_client_stats },
        { "run-count",                  1, 0, 'x' },
        { "debug",                      0, 0, 'D' },
        { "show-config",                0, 0, o_show_config },
        { "hide-histogram",             0, 0, o_hide_histogram },
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
        { "help",                       0, 0, 'h' },
        { "version",                    0, 0, 'v' },
        { NULL,                         0, 0, 0 }
    };

    int option_index;
    int c;
    char *endptr;
    while ((c = getopt_long(argc, argv, 
                "s:S:p:P:o:x:DRn:c:t:d:a:h", long_options, &option_index)) != -1)
    {
        switch (c) {
                case 'h':
                    return -1;
                    break;
                case 'v':
                    puts(PACKAGE_STRING);
                    puts("Copyright (C) 2011-2013 Garantia Data Ltd.");
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
                case 'P':
                    if (strcmp(optarg, "memcache_text") &&
                        strcmp(optarg, "memcache_binary") &&
                        strcmp(optarg, "redis")) {
                                fprintf(stderr, "error: supported protocols are 'memcache_text', 'memcache_binary' and 'redis'.\n");
                                return -1;
                    }
                    cfg->protocol = optarg;
                    break;
                case 'o':
                    cfg->out_file = optarg;
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
                case 'n':
                    endptr = NULL;
                    cfg->requests = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->requests || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: requests must be greater than zero.\n");
                        return -1;
                    }
                    if (cfg->test_time) {
                        fprintf(stderr, "error: --test-time and --requests are mutually exclusive.\n");
                        return -1;
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
                        fprintf(stderr, "error: data-size-range must be expressed as [0-n]-[1-n].\n");
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
                    cfg->key_minimum = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (cfg->key_minimum < 1 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-minimum must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_maximum:
                    endptr = NULL;
                    cfg->key_maximum = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (cfg->key_maximum< 1 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-maximum must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_stddev:
                    endptr = NULL;
                    cfg->key_stddev = (unsigned int) strtof(optarg, &endptr);
                    if (cfg->key_stddev<= 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-stddev must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_median:
                    endptr = NULL;
                    cfg->key_median = (unsigned int) strtof(optarg, &endptr);
                    if (cfg->key_median<= 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-median must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_pattern:
                    cfg->key_pattern = optarg;
                    if (strlen(cfg->key_pattern) != 3 || cfg->key_pattern[1] != ':' ||
                        (cfg->key_pattern[0] != 'R' && cfg->key_pattern[0] != 'S' && cfg->key_pattern[0] != 'G') ||
                        (cfg->key_pattern[2] != 'R' && cfg->key_pattern[2] != 'S' && cfg->key_pattern[2] != 'G')) {
                            fprintf(stderr, "error: key-pattern must be in the format of [S/R/G]:[S/R/G].\n");
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
                default:
                    return -1;
                    break;
        }
    }

    return 0;
}

void usage() {
    fprintf(stderr, "Usage: memtier_benchmark [options]\n"
            "A memcache/redis NoSQL traffic generator and performance benchmarking tool.\n"
            "\n"
            "Connection and General Options:\n"
            "  -s, --server=ADDR              Server address (default: localhost)\n"
            "  -p, --port=PORT                Server port (default: 6379)\n"
            "  -S, --unix-socket=SOCKET       UNIX Domain socket name (default: none)\n"
            "  -P, --protocol=PROTOCOL        Protocol to use (default: redis).  Other\n"
            "                                 supported protocols are memcache_text,\n"
            "                                 memcache_binary.\n"
            "  -x, --run-count=NUMBER         Number of full-test iterations to perform\n"
            "  -D, --debug                    Print debug output\n"
            "      --client-stats=FILE        Produce per-client stats file\n"
            "      --out-file=FILE            Name of output file (default: stdout)\n"
            "      --show-config              Print detailed configuration before running\n"
            "      --hide-histogram           Don't print detailed latency histogram\n"
            "\n"
            "Test Options:\n"
            "  -n, --requests=NUMBER          Number of total requests per client (default: 10000)\n"
            "  -c, --clients=NUMBER           Number of clients per thread (default: 50)\n"
            "  -t, --threads=NUMBER           Number of threads (default: 4)\n"
            "      --test-time=SECS           Number of seconds to run the test\n"
            "      --ratio=RATIO              Set:Get ratio (default: 1:10)\n"
            "      --pipeline=NUMBER          Number of concurrent pipelined requests (default: 1)\n"
            "      --reconnect-interval=NUM   Number of requests after which re-connection is performed\n"
            "      --multi-key-get=NUM        Enable multi-key get commands, up to NUM keys (default: 0)\n"
            "  -a, --authenticate=CREDENTIALS Authenticate to redis using CREDENTIALS, which depending\n"
            "                                 on the protocol can be PASSWORD or USER:PASSWORD.\n"
            "      --select-db=DB             DB number to select, when testing a redis server\n"
            "\n"
            "Object Options:\n"
            "  -d  --data-size=SIZE           Object data size (default: 32)\n"
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
            "                                 G for Gaussian distribution, R for uniform Random, S for Sequential\n"
            "      --key-stddev               The standard deviation used in the Gaussian distribution\n"
            "                                 (default is key range / 6)\n"
            "      --key-median               The median point used in the Gaussian distribution\n"
            "                                 (default is the center of the key range)\n"
            "      --help                     Display this help\n"
            "      --version                  Display version information\n"
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

    // provide some feedback...
    unsigned int active_threads = 0;
    do {
        active_threads = 0;
        sleep(1);

        unsigned long int total_ops = 0;
        unsigned long int total_bytes = 0;
        unsigned long int duration = 0;
        unsigned long int total_latency = 0;
        
        for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
            if (!(*i)->m_finished)
                active_threads++;

            total_ops += (*i)->m_cg->get_total_ops();
            total_bytes += (*i)->m_cg->get_total_bytes();
            total_latency += (*i)->m_cg->get_total_latency();
            if ((*i)->m_cg->get_duration_usec() > duration)                
                duration = (*i)->m_cg->get_duration_usec();
        }
        
        unsigned long int ops_sec = 0;
        unsigned long int bytes_sec = 0;
        double avg_latency = 0;
        if (duration > 1000000) {
            ops_sec = (total_ops / (duration / 1000000));
            bytes_sec = (total_bytes / (duration / 1000000));
            avg_latency = ((double) total_latency / 1000 / total_ops) ;
        }

        char bytes_str[40];
        size_to_str(bytes_sec, bytes_str, sizeof(bytes_str)-1);
        
        fprintf(stderr, "[RUN #%u, %3u secs] %2u threads: %11lu ops, %7lu ops/sec, %s/sec, %5.2fmsec latency\r",
            run_id, (unsigned int) (duration / 1000000), active_threads, total_ops, ops_sec, bytes_str, avg_latency);
    } while (active_threads > 0);

    fprintf(stderr, "\n\n");

    // join all threads back and unify stats
    run_stats stats;
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


int main(int argc, char *argv[])
{   
    struct benchmark_config cfg;

    memset(&cfg, 0, sizeof(struct benchmark_config));
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
        
        obj_gen = new object_generator();
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
        if (strcmp(cfg.protocol, "redis") != 0  &&
            strcmp(cfg.protocol, "memcache_binary") != 0) {
                fprintf(stderr, "error: authenticate can only be used with redis or memcache_binary.\n");
                usage();
        }
        if (strcmp(cfg.protocol, "memcache_binary") == 0 &&
            strchr(cfg.authenticate, ':') == NULL) {
                fprintf(stderr, "error: binary_memcache credentials must be in the form of USER:PASSWORD.\n");
                usage();
        }
    }

    if (cfg.select_db > 0 && strcmp(cfg.protocol, "redis")) {
        fprintf(stderr, "error: select-db can only be used with redis protocol.\n");
        usage();
    }
    if (cfg.data_offset > 0) {
        if (cfg.data_offset > (1<<29)-1) {
            fprintf(stderr, "error: data-offset too long\n");
            usage();
        }
        if (cfg.expiry_range.min || cfg.expiry_range.max || strcmp(cfg.protocol, "redis")) {
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

    if (!cfg.data_import) {
        obj_gen->set_random_data(cfg.random_data);
    }
    
    if (!cfg.data_import || cfg.generate_keys) {
        obj_gen->set_key_prefix(cfg.key_prefix);
        obj_gen->set_key_range(cfg.key_minimum, cfg.key_maximum);
    }
    if (cfg.key_stddev>0 || cfg.key_median>0) {
        if (cfg.key_pattern[0]!='G' && cfg.key_pattern[2]!='G') {
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
        outfile = stdout;
    }

    if (!cfg.verify_only) {
        std::vector<run_stats> all_stats;
        for (unsigned int run_id = 1; run_id <= cfg.run_count; run_id++) {
            if (run_id > 1)
                sleep(1);   // let connections settle
            
            run_stats stats = run_benchmark(run_id, &cfg, obj_gen);

            all_stats.push_back(stats);
        }
        
        // Print some run information
        fprintf(outfile,
               "%-9u Threads\n"
               "%-9u Connections per thread\n"
               "%-9u %s\n",
               cfg.threads, cfg.clients, 
               cfg.requests > 0 ? cfg.requests : cfg.test_time,
               cfg.requests > 0 ? "Requests per thread"  : "Seconds");

        // If more than 1 run was used, compute best, worst and average
        if (cfg.run_count > 1) {
            unsigned int min_ops_sec = (unsigned int) -1;
            unsigned int max_ops_sec = 0;
            run_stats* worst = NULL;
            run_stats* best = NULL;        
            for (std::vector<run_stats>::iterator i = all_stats.begin(); i != all_stats.end(); i++) {
                unsigned int secs = (i->get_duration_usec() / 1000);
                unsigned int ops_sec = (i->get_total_ops() / (secs > 0 ? secs : 1)) * 1000;
                if (ops_sec < min_ops_sec) {
                    min_ops_sec = ops_sec;                
                    worst = &(*i);
                }
                if (ops_sec > max_ops_sec) {
                    max_ops_sec = ops_sec;
                    best = &(*i);
                }
            }


            fprintf(outfile, "\n\n"
                             "BEST RUN RESULTS\n"
                             "========================================================================\n");        
            best->print(outfile, !cfg.hide_histogram);

            fprintf(outfile, "\n\n"
                             "WORST RUN RESULTS\n"
                             "========================================================================\n");        
            worst->print(outfile, !cfg.hide_histogram);

            fprintf(outfile, "\n\n"
                             "AGGREGATED AVERAGE RESULTS (%u runs)\n"
                             "========================================================================\n", cfg.run_count);

            run_stats average;
            average.aggregate_average(all_stats);
            average.print(outfile, !cfg.hide_histogram);
        } else {
            all_stats.begin()->print(outfile, !cfg.hide_histogram);
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

        // Clean up...
        delete client;
        delete verify_protocol;
        event_base_free(verify_event_base);
    }

    if (outfile != stdout) {
        fclose(outfile);
    }

    delete obj_gen;
    if (keylist != NULL)
        delete keylist;
}
