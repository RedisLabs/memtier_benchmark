# Development Guide

This document provides information for developers working on memtier_benchmark.

## Building from Source

### Prerequisites

The following libraries are required for building:

* libevent 2.0.10 or newer.
* OpenSSL (unless TLS support is disabled by `./configure --disable-tls`).

The following tools are required:
* autoconf
* automake
* pkg-config
* GNU make
* GCC C++ compiler

### CentOS/Red Hat Linux 7 or newer

Use the following to install prerequisites:
```
$ sudo yum install autoconf automake make gcc-c++ \
    zlib-devel libmemcached-devel libevent-devel openssl-devel
```

### Ubuntu/Debian

Use the following to install prerequisites:

```
$ sudo apt-get install build-essential autoconf automake \
    libevent-dev pkg-config zlib1g-dev libssl-dev
```

### macOS

To build natively on macOS, use Homebrew to install the required dependencies:

```
$ brew install autoconf automake libtool libevent pkg-config openssl@3.0
```

When running `./configure`, if it fails to find libssl it may be necessary to
tweak the `PKG_CONFIG_PATH` environment variable:

```
PKG_CONFIG_PATH=`brew --prefix openssl@3.0`/lib/pkgconfig ./configure
```

### Building and Installing

After downloading the source tree, use standard autoconf/automake commands:

```
$ autoreconf -ivf
$ ./configure
$ make
$ sudo make install
```

## Testing

The project includes a basic set of integration tests.

### Integration Tests

Integration tests are based on [RLTest](https://github.com/RedisLabsModules/RLTest), and specific setup parameters can be provided
to configure tests and topologies (OSS standalone and OSS cluster). By default the tests will be ran for all common commands, and with OSS standalone setup.

To run all integration tests in a Python virtualenv, follow these steps:

    $ mkdir -p .env
    $ virtualenv .env
    $ source .env/bin/activate
    $ pip install -r tests/test_requirements.txt
    $ ./tests/run_tests.sh

To understand what test options are available simply run:

    $ ./tests/run_tests.sh --help

### Memory Leak Detection with Sanitizers

memtier_benchmark supports building with AddressSanitizer (ASAN) and LeakSanitizer (LSAN) to detect memory errors and leaks during testing.

To build with sanitizers enabled:

    $ ./configure --enable-sanitizers
    $ make

To run tests with leak detection:

    $ ASAN_OPTIONS=detect_leaks=1 ./tests/run_tests.sh

If memory leaks or errors are detected, tests will fail with detailed error messages showing the location of the issue.

To verify ASAN is enabled:

    $ ldd ./memtier_benchmark | grep asan

### Undefined Behavior Detection with UBSan

memtier_benchmark supports building with UndefinedBehaviorSanitizer (UBSan) to detect undefined behavior such as integer overflows, null pointer dereferences, and alignment issues.

To build with UBSan enabled:

    $ ./configure --enable-ubsan
    $ make

To run tests with undefined behavior detection:

    $ UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 ./tests/run_tests.sh

UBSan can be combined with ASAN for comprehensive testing:

    $ ./configure --enable-sanitizers --enable-ubsan
    $ make

**Note:** UBSan can be used together with ASAN/LSAN, but not with ThreadSanitizer (TSAN).

### Data Race Detection with Thread Sanitizer

memtier_benchmark supports building with ThreadSanitizer (TSAN) to detect data races and threading issues.

To build with Thread Sanitizer enabled:

    $ ./configure --enable-thread-sanitizer
    $ make

To run tests with race detection (requires disabling ASLR on kernel 6.6+):

    $ TSAN_OPTIONS="suppressions=$(pwd)/tsan_suppressions.txt" setarch `uname -m` -R ./tests/run_tests.sh

To verify TSAN is enabled:

    $ ldd ./memtier_benchmark | grep tsan

**Note:** TSAN and ASAN are mutually exclusive and cannot be used together. A suppression file (`tsan_suppressions.txt`) is provided to ignore known benign data races that do not affect correctness.

