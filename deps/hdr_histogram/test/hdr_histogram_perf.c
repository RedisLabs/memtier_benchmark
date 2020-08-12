/**
 * hdr_histogram_perf.c
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#include <stdint.h>
#include <stdlib.h>

#include <stdio.h>
#include <hdr_histogram.h>
#include <string.h>

#include "hdr_time.h"

#if defined(_WIN32) || defined(_WIN64) || defined(__CYGWIN__)

#if !defined(WIN32_LEAN_AND_MEAN)
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
#define snprintf sprintf_s

#endif


static hdr_timespec diff(hdr_timespec start, hdr_timespec end)
{
    hdr_timespec temp;
    if ((end.tv_nsec-start.tv_nsec) < 0)
    {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec-start.tv_nsec;
    }
    else
    {
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
}

/* Formats the given double with 2 dps, and , thousand separators */
static char *format_double(double d)
{
    int p;
    static char buffer[30];

    snprintf(buffer, sizeof(buffer), "%0.2f", d);

    p = (int) strlen(buffer) - 6;

    while (p > 0)
    {
        memmove(&buffer[p + 1], &buffer[p], strlen(buffer) - p + 1);
        buffer[p] = ',';

        p = p - 3;
    }

    return buffer;
}

int main()
{
    struct hdr_histogram* histogram;
    hdr_timespec t0, t1;
    int result, i;
    int64_t iterations;
    int64_t max_value = INT64_C(24) * 60 * 60 * 1000000;
    int64_t min_value = 1;

    result = hdr_init(min_value, max_value, 4, &histogram);
    if (result != 0)
    {
        fprintf(stderr, "Failed to allocate histogram: %d\n", result);
        return -1;
    }

    iterations = 400000000;

    for (i = 0; i < 100; i++)
    {
        int64_t j;
        hdr_timespec taken;
        double time_taken, ops_sec;

        hdr_gettime(&t0);
        for (j = 1; j < iterations; j++)
        {
            hdr_record_value(histogram, j);
        }
        hdr_gettime(&t1);

        taken = diff(t0, t1);
        time_taken = taken.tv_sec + taken.tv_nsec / 1000000000.0;
        ops_sec = (iterations - 1) / time_taken;

        printf("%s - %d, ops/sec: %s\n", "Iteration", i + 1, format_double(ops_sec));
    }

    return 0;
}
