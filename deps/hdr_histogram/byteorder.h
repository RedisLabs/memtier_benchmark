/**
 * Copyright (c) 2012, 2013, 2014 Gil Tene
 * Copyright (c) 2014 Michael Barker
 * Copyright (c) 2014 Matt Warren
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <sys/isa_defs.h>
# include <sys/byteorder.h>

# define betoh16(x) BE_16(x)
# define letoh16(x) LE_16(x)
# define betoh32(x) BE_32(x)
# define letoh32(x) LE_32(x)
# define betoh64(x) BE_64(x)
# define letoh64(x) LE_64(x)
#define htobe16(x) BE_16(x)
#define be16toh(x) BE_16(x)
#define htobe32(x) BE_32(x)
#define be32toh(x) BE_32(x)
#define htobe64(x) BE_64(x)
#define be64toh(x) BE_64(x)
// Solaris defines endian by setting _LITTLE_ENDIAN or _BIG_ENDIAN
# ifdef _BIG_ENDIAN
#  define IS_BIG_ENDIAN
# endif
# ifdef _LITTLE_ENDIAN
#  define IS_LITTLE_ENDIAN
# endif
// Make sure we got some kind of endian (but not both)
#if defined(IS_BIG_ENDIAN) == defined(IS_LITTLE_ENDIAN)
# error "Failed to get endian type for this system"
#endif

// Define bswap functions if we didn't get any from the system headers
#ifndef BSWAP_16
# define BSWAP_16(x) ( \
                    ((uint16_t)(x) & 0x00ffU) << 8 | \
                    ((uint16_t)(x) & 0xff00U) >> 8)
#endif
#ifndef BSWAP_32
# define BSWAP_32(x) ( \
                    ((uint32_t)(x) & 0x000000ffU) << 24 | \
                    ((uint32_t)(x) & 0x0000ff00U) << 8 | \
                    ((uint32_t)(x) & 0x00ff0000U) >> 8 | \
                    ((uint32_t)(x) & 0xff000000U) >> 24)
#endif
#ifndef BSWAP_64
# define BSWAP_64(x) ( \
                    ((uint64_t)(x) & 0x00000000000000ffULL) << 56 | \
                    ((uint64_t)(x) & 0x000000000000ff00ULL) << 40 | \
                    ((uint64_t)(x) & 0x0000000000ff0000ULL) << 24 | \
                    ((uint64_t)(x) & 0x00000000ff000000ULL) << 8 | \
                    ((uint64_t)(x) & 0x000000ff00000000ULL) >> 8 | \
                    ((uint64_t)(x) & 0x0000ff0000000000ULL) >> 24 | \
                    ((uint64_t)(x) & 0x00ff000000000000ULL) >> 40 | \
                    ((uint64_t)(x) & 0xff00000000000000ULL) >> 56)
#endif

// Define conversion functions if we didn't get any from the system headers
#ifndef BE_16
// Big endian system, swap when converting to/from little endian
# if defined IS_BIG_ENDIAN
#  define BE_16(x) (x)
#  define BE_32(x) (x)
#  define BE_64(x) (x)
#  define LE_16(x) BSWAP_16(x)
#  define LE_32(x) BSWAP_32(x)
#  define LE_64(x) BSWAP_64(x)
// Little endian system, swap when converting to/from big endian
# elif defined IS_LITTLE_ENDIAN
#  define BE_16(x) BSWAP_16(x)
#  define BE_32(x) BSWAP_32(x)
#  define BE_64(x) BSWAP_64(x)
#  define LE_16(x) (x)
#  define LE_32(x) (x)
#  define LE_64(x) (x)
#endif
#endif
