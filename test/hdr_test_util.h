/**
 * hdr_test_util.h
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#ifndef HDR_HISTOGRAM_HDR_TEST_UTIL_H
#define HDR_HISTOGRAM_HDR_TEST_UTIL_H

static char* compare_histograms(struct hdr_histogram* expected, struct hdr_histogram* actual)
{
    struct hdr_iter expected_iter;
    struct hdr_iter actual_iter;

    hdr_iter_init(&expected_iter, expected);
    hdr_iter_init(&actual_iter, actual);

    while (hdr_iter_next(&expected_iter))
    {
        mu_assert("Should have next", hdr_iter_next(&actual_iter));
        mu_assert("counts mismatch", compare_int64(expected_iter.count, actual_iter.count));
    }

    mu_assert("Min mismatch", compare_int64(expected->min_value, actual->min_value));
    mu_assert("Max mismatch", compare_int64(expected->max_value, actual->max_value));
    mu_assert("Total mismatch", compare_int64(expected->total_count, actual->total_count));

    return 0;
}


#endif
