#include <stdio.h>

#include "minunit.h"

bool compare_double(double a, double b, double delta)
{
    if (fabs(a - b) < delta)
    {
        return true;
    }
    
    printf("[compare_double] fabs(%f, %f) < %f == false\n", a, b, delta);
    return false;
}

bool compare_int64(int64_t a, int64_t b)
{
    if (a == b)
    {
        return true;
    }
    
    printf("[compare_int64] %" PRIu64 " == %" PRIu64 " == false\n", a, b);
    return false;
}
