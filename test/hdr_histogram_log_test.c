/**
 * hdr_histogram_log_test.c
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <time.h>

#include <stdio.h>
#include "hdr_time.h"
#include <hdr_histogram.h>
#include <hdr_histogram_log.h>
#include <hdr_encoding.h>
#include "minunit.h"

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4996)
#endif

int tests_run = 0;

static bool compare_int(int a, int b)
{
    if (a == b)
    {
        return true;
    }

    printf("%d != %d\n", a, b);
    return false;
}

static long ns_to_ms(long ns)
{
    return (ns / 1000000) * 1000000;
}

static bool compare_timespec(hdr_timespec* a, hdr_timespec* b)
{
    char a_str[128];
    char b_str[128];

    long a_tv_msec = ns_to_ms(a->tv_nsec);
    long b_tv_msec = ns_to_ms(b->tv_nsec);

    /* Allow off by 1 millisecond due to parsing and rounding. */
    if (a->tv_sec == b->tv_sec && labs(a_tv_msec - b_tv_msec) <= 1000000)
    {
        return true;
    }

    if (a->tv_sec != b->tv_sec)
    {
#if defined(_MSC_VER)
        _ctime32_s(a_str, sizeof(a_str), &a->tv_sec);
        _ctime32_s(b_str, sizeof(b_str), &b->tv_sec);
        printf("tv_sec: %s != %s\n", a_str, b_str);
#else
        printf(
            "tv_sec: %s != %s\n",
            ctime_r(&a->tv_sec, a_str),
            ctime_r(&b->tv_sec, b_str));
#endif
    }

    if (a_tv_msec == b_tv_msec)
    {
        printf("%ld != %ld\n", a->tv_nsec, b->tv_nsec);
    }

    return false;
}

static bool compare_string(const char* a, const char* b, size_t len)
{
    if (strncmp(a, b, len) == 0)
    {
        return true;
    }

    printf("%s != %s\n", a, b);
    return false;
}

static bool compare_histogram(struct hdr_histogram* a, struct hdr_histogram* b)
{
    int64_t a_max, b_max, a_min, b_min;
    size_t a_size, b_size, counts_size;
    struct hdr_iter iter_a, iter_b;

    if (a->counts_len != b->counts_len)
    {
        printf(
            "a.counts_len = %"PRIu32", b.counts_len = %"PRIu32"\n",
            a->counts_len, b->counts_len);
        return false;
    }

    a_max = hdr_max(a);
    b_max = hdr_max(b);

    if (a_max != b_max)
    {
        printf("a.max = %"PRIu64", b.max = %"PRIu64"\n", a_max, b_max);
    }

    a_min = hdr_min(a);
    b_min = hdr_min(b);

    if (a_min != b_min)
    {
        printf("a.min = %"PRIu64", b.min = %"PRIu64"\n", a_min, b_min);
    }

    a_size = hdr_get_memory_size(a);
    b_size = hdr_get_memory_size(b);

    if (a_size != b_size)
    {
        printf("a.size: %u, b.size: %u\n", (unsigned) a_size, (unsigned) b_size);
        return false;
    }

    counts_size = a->counts_len * sizeof(int64_t);

    if (memcmp(a->counts, b->counts, counts_size) == 0)
    {
        return true;
    }

    printf("%s\n", "Counts incorrect");

    hdr_iter_init(&iter_a, a);
    hdr_iter_init(&iter_b, b);

    while (hdr_iter_next(&iter_a) && hdr_iter_next(&iter_b))
    {
        if (iter_a.count != iter_b.count ||
            iter_a.value != iter_b.value)
        {
            printf(
                "A - value: %"PRIu64", count: %"PRIu64", B - value: %"PRIu64", count: %"PRIu64"\n",
                iter_a.value, iter_a.count,
                iter_b.value, iter_b.count);
        }
    }

    return false;
}

static struct hdr_histogram* raw_histogram = NULL;
static struct hdr_histogram* cor_histogram = NULL;

static void load_histograms()
{
    int i;

    free(raw_histogram);
    free(cor_histogram);

    hdr_alloc(INT64_C(3600) * 1000 * 1000, 3, &raw_histogram);
    hdr_alloc(INT64_C(3600) * 1000 * 1000, 3, &cor_histogram);

    for (i = 0; i < 10000; i++)
    {
        hdr_record_value(raw_histogram, 1000);
        hdr_record_corrected_value(cor_histogram, 1000, 10000);
    }

    hdr_record_value(raw_histogram, 100000000);
    hdr_record_corrected_value(cor_histogram, 100000000, 10000);
}

static bool validate_return_code(int rc)
{
    if (rc == 0)
    {
        return true;
    }

    printf("%s\n", hdr_strerror(rc));
    return false;
}

/* Prototypes to avoid exporting in header file. */
void hdr_base64_encode_block(const uint8_t* input, char* output);

void hdr_base64_decode_block(const char* input, uint8_t* output);
int hdr_encode_compressed(struct hdr_histogram* h, uint8_t** buffer, size_t* length);
int hdr_decode_compressed(
    uint8_t* buffer, size_t length, struct hdr_histogram** histogram);
void hex_dump (char *desc, void *addr, int len);

static char* test_encode_and_decode_compressed()
{
    uint8_t* buffer = NULL;
    size_t len = 0;
    int rc = 0;
    struct hdr_histogram* actual = NULL;
    struct hdr_histogram* expected;

    load_histograms();

    expected = raw_histogram;

    rc = hdr_encode_compressed(expected, &buffer, &len);
    mu_assert("Did not encode", validate_return_code(rc));

    rc = hdr_decode_compressed(buffer, len, &actual);
    mu_assert("Did not decode", validate_return_code(rc));

    mu_assert("Loaded histogram is null", actual != NULL);

    mu_assert(
        "Comparison did not match",
        compare_histogram(expected, actual));

    free(actual);

    return 0;
}

static char* test_encode_and_decode_compressed2()
{
    uint8_t* buffer = NULL;
    size_t len = 0;
    int rc = 0;
    struct hdr_histogram* actual = NULL;
    struct hdr_histogram* expected;

    load_histograms();

    expected = cor_histogram;

    rc = hdr_encode_compressed(expected, &buffer, &len);
    mu_assert("Did not encode", validate_return_code(rc));

    rc = hdr_decode_compressed(buffer, len, &actual);
    mu_assert("Did not decode", validate_return_code(rc));

    mu_assert("Loaded histogram is null", actual != NULL);

    mu_assert(
            "Comparison did not match",
            compare_histogram(expected, actual));

    free(actual);

    return 0;
}

static char* test_bounds_check_on_decode()
{
    uint8_t* buffer = NULL;
    size_t len = 0;
    int rc = 0;
    struct hdr_histogram* actual = NULL;
    struct hdr_histogram* expected;

    load_histograms();

    expected = cor_histogram;

    rc = hdr_encode_compressed(expected, &buffer, &len);
    mu_assert("Did not encode", validate_return_code(rc));

    rc = hdr_decode_compressed(buffer, len - 1, &actual);
    mu_assert("Should have be invalid", compare_int64(EINVAL, rc));
    mu_assert("Should not have built histogram", NULL == actual);

    return 0;
}

static char* test_encode_and_decode_base64()
{
    uint8_t* buffer = NULL;
    uint8_t* decoded = NULL;
    char* encoded = NULL;
    size_t encoded_len, decoded_len;
    size_t len = 0;
    int rc = 0;

    load_histograms();

    rc = hdr_encode_compressed(cor_histogram, &buffer, &len);
    mu_assert("Did not encode", validate_return_code(rc));

    encoded_len = hdr_base64_encoded_len(len);
    decoded_len = hdr_base64_decoded_len(encoded_len);
    encoded = calloc(encoded_len + 1, sizeof(char));
    decoded = calloc(decoded_len, sizeof(uint8_t));

    hdr_base64_encode(buffer, len, encoded, encoded_len);
    hdr_base64_decode(encoded, encoded_len, decoded, decoded_len);

    mu_assert("Should be same", memcmp(buffer, decoded, len) == 0);

    return 0;
}

static char* test_encode_and_decode_empty()
{
    uint8_t* buffer;
    uint8_t* decoded;
    char* encoded;
    size_t len = 0;
    int rc = 0;
    size_t encoded_len;
    size_t decoded_len;

    free(raw_histogram);

    mu_assert("allocation should be valid", 0 == hdr_init(1, 1000000, 1, &raw_histogram));

    rc = hdr_encode_compressed(raw_histogram, &buffer, &len);
    mu_assert("Did not encode", validate_return_code(rc));

    encoded_len = hdr_base64_encoded_len(len);
    decoded_len = hdr_base64_decoded_len(encoded_len);
    encoded = calloc(encoded_len + 1, sizeof(char));
    decoded = calloc(decoded_len, sizeof(uint8_t));

    hdr_base64_encode(buffer, len, encoded, encoded_len);
    hdr_base64_decode(encoded, encoded_len, decoded, decoded_len);

    mu_assert("Should be same", memcmp(buffer, decoded, len) == 0);

    return 0;
}

static char* test_encode_and_decode_compressed_large()
{
    const int64_t limit = INT64_C(3600) * 1000 * 1000;
    struct hdr_histogram* actual = NULL;
    struct hdr_histogram* expected = NULL;
    uint8_t* buffer = NULL;
    size_t len = 0;
    int i;
    int rc = 0;

    hdr_init(1, limit, 4, &expected);
    srand(5);

    for (i = 0; i < 8070; i++)
    {
        hdr_record_value(expected, rand() % limit);
    }

    rc = hdr_encode_compressed(expected, &buffer, &len);
    mu_assert("Did not encode", validate_return_code(rc));

    rc = hdr_decode_compressed(buffer, len, &actual);
    mu_assert("Did not decode", validate_return_code(rc));

    mu_assert("Loaded histogram is null", actual != NULL);

    mu_assert(
        "Comparison did not match",
        compare_histogram(expected, actual));

    free(expected);
    free(actual);

    return 0;
}


static bool assert_base64_encode(const char* input, const char* expected)
{
    size_t input_len = strlen(input);
    int output_len = (int) (ceil(input_len / 3.0) * 4.0);

    char* output = calloc(sizeof(char), output_len);

    int r = hdr_base64_encode((uint8_t*)input, input_len, output, output_len);
    bool result = r == 0 && compare_string(expected, output, output_len);

    free(output);

    return result;
}

static char* base64_encode_encodes_without_padding()
{
    mu_assert(
        "Encoding without padding",
        assert_base64_encode(
            "any carnal pleasur",
            "YW55IGNhcm5hbCBwbGVhc3Vy"));

    return 0;
}

static char* base64_encode_encodes_with_padding()
{
    mu_assert(
        "Encoding with padding '='",
        assert_base64_encode(
            "any carnal pleasure.",
            "YW55IGNhcm5hbCBwbGVhc3VyZS4="));
    mu_assert(
        "Encoding with padding '=='",
        assert_base64_encode(
            "any carnal pleasure",
            "YW55IGNhcm5hbCBwbGVhc3VyZQ=="));

    return 0;
}

static char* base64_encode_fails_with_invalid_lengths()
{
    mu_assert(
        "Output length not 4/3 of input length",
        hdr_base64_encode(NULL, 9, NULL, 11));

    return 0;
}

static char* base64_encode_block_encodes_3_bytes()
{
    char output[5] = { 0 };

    hdr_base64_encode_block((uint8_t*)"Man", output);
    mu_assert("Encoding", compare_string("TWFu", output, 4));

    return 0;
}

static char* base64_decode_block_decodes_4_chars()
{
    uint8_t output[4] = { 0 };

    hdr_base64_decode_block("TWFu", output);
    mu_assert("Decoding", compare_string("Man", (char*) output, 3));

    return 0;
}

static bool assert_base64_decode(const char* base64_encoded, const char* expected)
{
    size_t encoded_len = strlen(base64_encoded);
    size_t output_len = (encoded_len / 4) * 3;

    uint8_t* output = calloc(sizeof(uint8_t), output_len);

    int result = hdr_base64_decode(base64_encoded, encoded_len, output, output_len);

    return result == 0 && compare_string(expected, (char*)output, output_len);
}

static char* base64_decode_decodes_strings_without_padding()
{
    mu_assert(
        "Encoding without padding",
        assert_base64_decode(
            "YW55IGNhcm5hbCBwbGVhc3Vy",
            "any carnal pleasur"));

    return 0;
}

static char* base64_decode_decodes_strings_with_padding()
{
    mu_assert(
        "Encoding with padding '='",
        assert_base64_decode(
            "YW55IGNhcm5hbCBwbGVhc3VyZS4=",
            "any carnal pleasure."));

    mu_assert(
        "Encoding with padding '=='",
        assert_base64_decode(
            "YW55IGNhcm5hbCBwbGVhc3VyZQ==",
            "any carnal pleasure"));

    return 0;
}

static char* base64_decode_fails_with_invalid_lengths()
{
    mu_assert("Input length % 4 != 0", hdr_base64_decode(NULL, 5, NULL, 3) != 0);
    mu_assert("Input length < 4", hdr_base64_decode(NULL, 3, NULL, 3) != 0);
    mu_assert(
        "Output length not 3/4 of input length",
        hdr_base64_decode(NULL, 8, NULL, 7) != 0);

    return 0;
}

static char* writes_and_reads_log()
{
    struct hdr_log_writer writer;
    struct hdr_log_reader reader;
    struct hdr_histogram* read_cor_histogram;
    struct hdr_histogram* read_raw_histogram;
    const char* file_name = "histogram.log";
    int rc = 0;
    FILE* log_file;
    const char* tag = "tag_value";
    char read_tag[32];
    struct hdr_log_entry write_entry;
    struct hdr_log_entry read_entry;

    hdr_gettime(&write_entry.start_timestamp);

    write_entry.interval.tv_sec = 5;
    write_entry.interval.tv_nsec = 2000000;

    hdr_log_writer_init(&writer);
    hdr_log_reader_init(&reader);

    log_file = fopen(file_name, "w+");

    rc = hdr_log_write_header(&writer, log_file, "Test log", &write_entry.start_timestamp);
    mu_assert("Failed header write", validate_return_code(rc));

    write_entry.tag = (char *) tag;
    write_entry.tag_len = strlen(tag);
    hdr_log_write_entry(&writer, log_file, &write_entry, cor_histogram);
    mu_assert("Failed corrected write", validate_return_code(rc));

    write_entry.tag = NULL;
    hdr_log_write_entry(&writer, log_file, &write_entry, raw_histogram);
    mu_assert("Failed raw write", validate_return_code(rc));

    fprintf(log_file, "\n");

    fflush(log_file);
    fclose(log_file);

    log_file = fopen(file_name, "r");

    read_cor_histogram = NULL;
    read_raw_histogram = NULL;

    rc = hdr_log_read_header(&reader, log_file);
    mu_assert("Failed header read", validate_return_code(rc));
    mu_assert("Incorrect major version", compare_int(reader.major_version, 1));
    mu_assert("Incorrect minor version", compare_int(reader.minor_version, 2));
    mu_assert(
        "Incorrect start timestamp",
        compare_timespec(&reader.start_timestamp, &write_entry.start_timestamp));

    read_entry.tag = read_tag;
    read_entry.tag_len = sizeof(read_tag);
    read_entry.tag[0] = '\0';
    read_entry.tag[read_entry.tag_len - 1] = '\0';

    rc = hdr_log_read_entry(&reader, log_file, &read_entry, &read_cor_histogram);
    mu_assert("Failed corrected read", validate_return_code(rc));
    mu_assert(
        "Incorrect first timestamp", compare_timespec(&read_entry.start_timestamp, &write_entry.start_timestamp));
    mu_assert(
        "Incorrect first interval", compare_timespec(&read_entry.interval, &write_entry.interval));
    mu_assert("Incorrect tag", compare_string(read_entry.tag, tag, strlen(tag)));

    read_entry.tag[0] = '\0';
    read_entry.tag[read_entry.tag_len - 1] = '\0';
    rc = hdr_log_read_entry(&reader, log_file, &read_entry, &read_raw_histogram);
    mu_assert("Should not find tag", read_entry.tag[0] == '\0');
    mu_assert("Failed raw read", validate_return_code(rc));

    mu_assert(
        "Histograms do not match",
        compare_histogram(cor_histogram, read_cor_histogram));

    mu_assert(
        "Histograms do not match",
        compare_histogram(raw_histogram, read_raw_histogram));

    rc = hdr_log_read(&reader, log_file, &read_cor_histogram, NULL, NULL);
    mu_assert("No EOF at end of file", rc == EOF);

    fclose(log_file);
    remove(file_name);

    return 0;
}

static char* log_reader_aggregates_into_single_histogram()
{
    const char* file_name = "histogram.log";
    hdr_timespec timestamp;
    hdr_timespec interval;
    struct hdr_log_writer writer;
    struct hdr_log_reader reader;
    int rc = 0;
    FILE* log_file;
    struct hdr_histogram* histogram;
    struct hdr_iter iter;
    int64_t expected_total_count;

    hdr_gettime(&timestamp);
    interval.tv_sec = 5;
    interval.tv_nsec = 2000000;

    hdr_log_writer_init(&writer);
    hdr_log_reader_init(&reader);

    log_file = fopen(file_name, "w+");

    hdr_log_write_header(&writer, log_file, "Test log", &timestamp);
    hdr_log_write(&writer, log_file, &timestamp, &interval, cor_histogram);
    hdr_log_write(&writer, log_file, &timestamp, &interval, raw_histogram);
    fflush(log_file);
    fclose(log_file);

    log_file = fopen(file_name, "r");

    hdr_alloc(INT64_C(3600) * 1000 * 1000, 3, &histogram);

    rc = hdr_log_read_header(&reader, log_file);
    mu_assert("Failed header read", validate_return_code(rc));
    rc = hdr_log_read(&reader, log_file, &histogram, NULL, NULL);
    mu_assert("Failed corrected read", validate_return_code(rc));
    rc = hdr_log_read(&reader, log_file, &histogram, NULL, NULL);
    mu_assert("Failed raw read", validate_return_code(rc));

    hdr_iter_recorded_init(&iter, histogram);
    expected_total_count = raw_histogram->total_count + cor_histogram->total_count;

    mu_assert(
        "Total counts incorrect",
        compare_int64(histogram->total_count, expected_total_count));

    while (hdr_iter_next(&iter))
    {
        int64_t count = iter.count;
        int64_t value = iter.value;

        int64_t expected_count =
            hdr_count_at_value(raw_histogram, value) +
            hdr_count_at_value(cor_histogram, value);

        mu_assert("Incorrect count", compare_int64(count, expected_count));
    }

    fclose(log_file);
    remove(file_name);
    free(histogram);

    return 0;
}

static char* log_reader_fails_with_incorrect_version()
{
    const char* log_with_invalid_version =
    "#[Test log]\n"
    "#[Histogram log format version 1.04]\n"
    "#[StartTime: 1404700005.222 (seconds since epoch), Mon Jul 02:26:45 GMT 2014]\n"
    "StartTimestamp\",\"EndTimestamp\",\"Interval_Max\",\"Interval_Compressed_Histogram\"\n";
    const char* file_name = "histogram_with_invalid_version.log";
    struct hdr_log_reader reader;
    FILE* log_file;
    int r;

    log_file = fopen(file_name, "w+");
    fprintf(log_file, "%s", log_with_invalid_version);
    fflush(log_file);
    fclose(log_file);

    log_file = fopen(file_name, "r");
    hdr_log_reader_init(&reader);
    r = hdr_log_read_header(&reader, log_file);

    mu_assert("Should error with incorrect version", r == HDR_LOG_INVALID_VERSION);

    fclose(log_file);
    remove(file_name);

    return 0;
}

static char* test_encode_decode_empty()
{
    char *data;
    struct hdr_histogram *histogram, *hdr_new = NULL;

    hdr_alloc(INT64_C(3600) * 1000 * 1000, 3, &histogram);

    mu_assert("Failed to encode histogram data", hdr_log_encode(histogram, &data) == 0);
    mu_assert("Failed to decode histogram data", hdr_log_decode(&hdr_new, data, strlen(data)) == 0);
    mu_assert("Histograms should be the same", compare_histogram(histogram, hdr_new));
    free(histogram);
    free(hdr_new);
    free(data);
    return 0;
}

static char* test_string_encode_decode()
{
    int i;
    char *data;
    struct hdr_histogram *histogram, *hdr_new = NULL;

    hdr_alloc(INT64_C(3600) * 1000 * 1000, 3, &histogram);

    for (i = 1; i < 100; i++)
    {
        hdr_record_value(histogram, i*i);
    }

    mu_assert("Failed to encode histogram data", hdr_log_encode(histogram, &data) == 0);
    mu_assert("Failed to decode histogram data", hdr_log_decode(&hdr_new, data, strlen(data)) == 0);
    mu_assert("Histograms should be the same", compare_histogram(histogram, hdr_new));
    mu_assert("Mean different after encode/decode", compare_double(hdr_mean(histogram), hdr_mean(hdr_new), 0.001));

    return 0;
}

static char* test_string_encode_decode_2()
{
    int i;
    char *data;

    struct hdr_histogram *histogram, *hdr_new = NULL;

    hdr_alloc(1000, 3, &histogram);

    for (i = 1; i < histogram->highest_trackable_value; i++)
    {
        hdr_record_value(histogram, i);
    }

    mu_assert(
        "Failed to encode histogram data", validate_return_code(hdr_log_encode(histogram, &data)));
    mu_assert(
        "Failed to decode histogram data", validate_return_code(hdr_log_decode(&hdr_new, data, strlen(data))));
    mu_assert("Histograms should be the same", compare_histogram(histogram, hdr_new));
    mu_assert("Mean different after encode/decode", compare_double(hdr_mean(histogram), hdr_mean(hdr_new), 0.001));

    return 0;
}


static char* decode_v1_log()
{
    const char* v1_log = "jHiccup-2.0.6.logV1.hlog";
    struct hdr_histogram* accum;
    struct hdr_histogram* h = NULL;
    struct hdr_log_reader reader;
    hdr_timespec timestamp, interval;
    int rc;
    int64_t total_count = 0;
    int histogram_count = 0;

    FILE* f = fopen(v1_log, "r");
    mu_assert("Can not open v1 log file", f != NULL);

    hdr_init(1, INT64_C(3600000000000), 3, &accum);


    hdr_log_reader_init(&reader);

    rc = hdr_log_read_header(&reader, f);
    mu_assert("Failed to read header", rc == 0);

    while ((rc = hdr_log_read(&reader, f, &h, &timestamp, &interval)) != EOF)
    {
        int64_t dropped;

        mu_assert("Failed to read histogram", rc == 0);
        histogram_count++;
        total_count += h->total_count;
        dropped = hdr_add(accum, h);
        mu_assert("Dropped events", compare_int64(dropped, 0));

        free(h);
        h = NULL;
    }

    mu_assert("Wrong number of histograms", compare_int(histogram_count, 88));
    mu_assert("Wrong total count", compare_int64(total_count, 65964));
    mu_assert("99.9 percentile wrong", compare_int64(1829765119, hdr_value_at_percentile(accum, 99.9)));
    mu_assert("max value wrong", compare_int64(1888485375, hdr_max(accum)));
    mu_assert("Seconds wrong", compare_int64(1438867590, reader.start_timestamp.tv_sec));
    mu_assert("Nanoseconds wrong", compare_int64(285000000, reader.start_timestamp.tv_nsec));

    return 0;
}


static char* decode_v2_log()
{
    struct hdr_histogram* accum;
    struct hdr_histogram* h = NULL;
    struct hdr_log_reader reader;
    hdr_timespec timestamp, interval;
    int histogram_count = 0;
    int64_t total_count = 0;
    int rc;

    const char* v2_log = "jHiccup-2.0.7S.logV2.hlog";

    FILE* f = fopen(v2_log, "r");
    mu_assert("Can not open v2 log file", f != NULL);

    hdr_init(1, INT64_C(3600000000000), 3, &accum);

    hdr_log_reader_init(&reader);

    rc = hdr_log_read_header(&reader, f);
    mu_assert("Failed to read header", validate_return_code(rc));

    while ((rc = hdr_log_read(&reader, f, &h, &timestamp, &interval)) != EOF)
    {
        int64_t dropped;

        mu_assert("Failed to read histogram", validate_return_code(rc));
        histogram_count++;
        total_count += h->total_count;
        dropped = hdr_add(accum, h);
        mu_assert("Dropped events", compare_int64(dropped, 0));

        free(h);
        h = NULL;
    }

    mu_assert("Wrong number of histograms", compare_int(histogram_count, 62));
    mu_assert("Wrong total count", compare_int64(total_count, 48761));
    mu_assert("99.9 percentile wrong", compare_int64(1745879039, hdr_value_at_percentile(accum, 99.9)));
    mu_assert("max value wrong", compare_int64(1796210687, hdr_max(accum)));
    mu_assert("Seconds wrong", compare_int64(1441812279, reader.start_timestamp.tv_sec));
    mu_assert("Nanoseconds wrong", compare_int64(474000000, reader.start_timestamp.tv_nsec));

    return 0;
}

static char* decode_v3_log()
{
    struct hdr_histogram* accum;
    struct hdr_histogram* h = NULL;
    struct hdr_log_reader reader;
    hdr_timespec timestamp;
    hdr_timespec interval;
    int rc;
    int histogram_count = 0;
    int64_t total_count = 0;

    const char* v3_log = "jHiccup-2.0.7S.logV3.hlog";

    FILE* f = fopen(v3_log, "r");
    if (NULL == f)
    {
        fprintf(stderr, "Open file: [%d] %s", errno, strerror(errno));
    }
    mu_assert("Can not open v3 log file", f != NULL);

    hdr_init(1, INT64_C(3600000000000), 3, &accum);

    hdr_log_reader_init(&reader);

    rc = hdr_log_read_header(&reader, f);
    mu_assert("Failed to read header", validate_return_code(rc));

    while ((rc = hdr_log_read(&reader, f, &h, &timestamp, &interval)) != EOF)
    {
        int64_t dropped;
        mu_assert("Failed to read histogram", validate_return_code(rc));
        histogram_count++;
        total_count += h->total_count;
        dropped = hdr_add(accum, h);
        mu_assert("Dropped events", compare_int64(dropped, 0));

        free(h);
        h = NULL;
    }

    mu_assert("Wrong number of histograms", compare_int(histogram_count, 62));
    mu_assert("Wrong total count", compare_int64(total_count, 48761));
    mu_assert("99.9 percentile wrong", compare_int64(1745879039, hdr_value_at_percentile(accum, 99.9)));
    mu_assert("max value wrong", compare_int64(1796210687, hdr_max(accum)));
    mu_assert("Seconds wrong", compare_int64(1441812279, reader.start_timestamp.tv_sec));
    mu_assert("Nanoseconds wrong", compare_int64(474000000, reader.start_timestamp.tv_nsec));

    return 0;
}

static int parse_line_from_file(const char* filename)
{
    struct hdr_histogram *h = NULL;
    hdr_timespec timestamp;
    hdr_timespec interval;
    int result;

    FILE* f = fopen(filename, "r");
    if (NULL == f)
    {
        fprintf(stderr, "Open file: [%d] %s", errno, strerror(errno));
        return -EIO;
    }

    result = hdr_log_read(NULL, f, &h, &timestamp, &interval);
    fclose(f);

    return result;
}

static char* handle_invalid_log_lines()
{
    mu_assert("Should have invalid histogram", -EINVAL == parse_line_from_file("test_tagged_invalid_histogram.txt"));
    mu_assert("Should have invalid tag key", -EINVAL == parse_line_from_file("test_tagged_invalid_tag_key.txt"));
    mu_assert("Should have invalid timestamp", -EINVAL == parse_line_from_file("test_tagged_invalid_timestamp.txt"));
    mu_assert("Should have missing histogram", -EINVAL == parse_line_from_file("test_tagged_missing_histogram.txt"));

    return 0;
}

static char* decode_v0_log()
{
    struct hdr_histogram* accum;
    const char* v1_log = "jHiccup-2.0.1.logV0.hlog";
    struct hdr_histogram* h = NULL;
    struct hdr_log_reader reader;
    hdr_timespec timestamp;
    hdr_timespec interval;
    int rc;
    int histogram_count = 0;
    int64_t total_count = 0;

    FILE* f = fopen(v1_log, "r");
    mu_assert("Can not open v1 log file", f != NULL);

    hdr_init(1, INT64_C(3600000000000), 3, &accum);

    hdr_log_reader_init(&reader);

    rc = hdr_log_read_header(&reader, f);
    mu_assert("Failed to read header", rc == 0);

    while ((rc = hdr_log_read(&reader, f, &h, &timestamp, &interval)) != EOF)
    {
        int64_t dropped;
        mu_assert("Failed to read histogram", rc == 0);
        histogram_count++;
        total_count += h->total_count;
        dropped = hdr_add(accum, h);
        mu_assert("Dropped events", compare_int64(dropped, 0));

        free(h);
        h = NULL;
    }

    mu_assert("Wrong number of histograms", compare_int(histogram_count, 81));
    mu_assert("Wrong total count", compare_int64(total_count, 61256));
    mu_assert("99.9 percentile wrong", compare_int64(1510998015, hdr_value_at_percentile(accum, 99.9)));
    mu_assert("max value wrong", compare_int64(1569718271, hdr_max(accum)));
    mu_assert("Seconds wrong", compare_int64(1438869961, reader.start_timestamp.tv_sec));
    mu_assert("Nanoseconds wrong", compare_int64(225000000, reader.start_timestamp.tv_nsec));

    return 0;
}

static struct mu_result all_tests()
{
    tests_run = 0;

    mu_run_test(test_encode_decode_empty);
    mu_run_test(test_encode_and_decode_compressed);
    mu_run_test(test_encode_and_decode_compressed2);
    mu_run_test(test_encode_and_decode_compressed_large);
    mu_run_test(test_encode_and_decode_base64);
    mu_run_test(test_bounds_check_on_decode);

    mu_run_test(base64_decode_block_decodes_4_chars);
    mu_run_test(base64_decode_fails_with_invalid_lengths);
    mu_run_test(base64_decode_decodes_strings_without_padding);
    mu_run_test(base64_decode_decodes_strings_with_padding);

    mu_run_test(base64_encode_block_encodes_3_bytes);
    mu_run_test(base64_encode_fails_with_invalid_lengths);
    mu_run_test(base64_encode_encodes_without_padding);
    mu_run_test(base64_encode_encodes_with_padding);

    mu_run_test(writes_and_reads_log);
    mu_run_test(log_reader_aggregates_into_single_histogram);
    mu_run_test(log_reader_fails_with_incorrect_version);

    mu_run_test(test_string_encode_decode);
    mu_run_test(test_string_encode_decode_2);

    mu_run_test(decode_v3_log);
    mu_run_test(decode_v2_log);
    mu_run_test(decode_v1_log);
    mu_run_test(decode_v0_log);
    mu_run_test(handle_invalid_log_lines);

    mu_run_test(test_encode_and_decode_empty);

    free(raw_histogram);
    free(cor_histogram);

    mu_ok;
}

static int hdr_histogram_log_run_tests()
{
    struct mu_result result = all_tests();

    if (result.message != 0)
    {
        printf("hdr_histogram_log_test.%s(): %s\n", result.test, result.message);
    }
    else
    {
        printf("ALL TESTS PASSED\n");
    }

    printf("Tests run: %d\n", tests_run);

    return result.message == NULL ? 0 : -1;
}

int main()
{
    return hdr_histogram_log_run_tests();
}

#if defined(_MSC_VER)
#pragma warning(pop)
#endif
