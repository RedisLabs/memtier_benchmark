/**
 * hdr_decoder.c
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <time.h>
#include <errno.h>
#include <string.h>
 
#include <hdr_histogram.h>
#include <hdr_histogram_log.h>

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4996)
#endif

int main(int argc, char** argv)
{
    int rc = 0;
    FILE* f;
    struct hdr_log_reader reader;
    struct hdr_histogram* h = NULL;
    hdr_timespec timestamp, interval;

    if (argc == 1)
    {
        f = stdin;
    }
    else
    {
        f = fopen(argv[1], "r");
    }

    if (!f)
    {
        fprintf(stderr, "Failed to open file(%s):%s\n", argv[1], strerror(errno));
        return -1;
    }

    if (hdr_log_reader_init(&reader))
    {
        fprintf(stderr, "Failed to init reader\n");
        return -1;
    }

    rc = hdr_log_read_header(&reader, f);
    if(rc)
    {
        fprintf(stderr, "Failed to read header: %s\n", hdr_strerror(rc));
        return -1;
    }

    while (true)
    {
        rc = hdr_log_read(&reader, f, &h, &timestamp, &interval);

        if (0 == rc)
        {
            hdr_percentiles_print(h, stdout, 5, 1.0, CLASSIC);
        }
        else if (EOF == rc)
        {
            break;
        }
        else
        {
            fprintf(stderr, "Failed to print histogram: %s\n", hdr_strerror(rc));
            return -1;
        }        
    }

    return 0;
}

#if defined(_MSC_VER)
#pragma warning(pop)
#endif
