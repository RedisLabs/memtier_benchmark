/**
 * hdr_histogram_test.c
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <errno.h>

#include <stdio.h>
#include <hdr_histogram.h>
#include <pthread.h>

#include "minunit.h"
#include "hdr_test_util.h"

int tests_run = 0;

struct test_histogram_data
{
    struct hdr_histogram* histogram;
    int64_t* values;
    int values_len;
};

static void* record_values(void* thread_context)
{
    int i;
    struct test_histogram_data* thread_data = (struct test_histogram_data*) thread_context;


    for (i = 0; i < thread_data->values_len; i++)
    {
        hdr_record_value_atomic(thread_data->histogram, thread_data->values[i]);
    }

    pthread_exit(NULL);
}


static char* test_recording_concurrently()
{
    const int value_count = 10000000;
    int64_t* values = calloc(value_count, sizeof(int64_t));
    struct hdr_histogram* expected_histogram;
    struct hdr_histogram* actual_histogram;
    struct test_histogram_data thread_data[2];
    struct hdr_iter expected_iter;
    struct hdr_iter actual_iter;
    pthread_t threads[2];
    int i;

    mu_assert("init", 0 == hdr_init(1, 10000000, 2, &expected_histogram));
    mu_assert("init", 0 == hdr_init(1, 10000000, 2, &actual_histogram));


    for (i = 0; i < value_count; i++)
    {
        values[i] = rand() % 20000;
    }
    
    for (i = 0; i < value_count; i++)
    {
        hdr_record_value(expected_histogram, values[i]);
    }

    thread_data[0].histogram = actual_histogram;
    thread_data[0].values = values;
    thread_data[0].values_len = value_count / 2;
    pthread_create(&threads[0], NULL, record_values, &thread_data[0]);

    thread_data[1].histogram = actual_histogram;
    thread_data[1].values = &values[value_count / 2];
    thread_data[1].values_len = value_count / 2;
    pthread_create(&threads[1], NULL, record_values, &thread_data[1]);

    pthread_join(threads[0], NULL);
    pthread_join(threads[1], NULL);

    hdr_iter_init(&expected_iter, expected_histogram);
    hdr_iter_init(&actual_iter, actual_histogram);

    return compare_histograms(expected_histogram, actual_histogram);
}

static struct mu_result all_tests()
{
    mu_run_test(test_recording_concurrently);

    mu_ok;
}

static int hdr_histogram_run_tests()
{
    struct mu_result result = all_tests();

    if (result.message != 0)
    {
        printf("hdr_histogram_test.%s(): %s\n", result.test, result.message);
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
    return hdr_histogram_run_tests();
}
