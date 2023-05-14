memtier_benchmark
=================
![GitHub](https://img.shields.io/github/license/RedisLabs/memtier_benchmark)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/RedisLabs/memtier_benchmark)](https://github.com/RedisLabs/memtier_benchmark/releases)
[![codecov](https://codecov.io/gh/RedisLabs/memtier_benchmark/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisLabs/memtier_benchmark)


memtier_benchmark is a command line utility developed by [Redis](https://redis.com) (formerly Garantia Data Ltd.) for load generation and bechmarking NoSQL key-value databases. It offers the following:

* Support for both Redis and Memcache protocols (text and binary)
* Multi-threaded multi-client execution
* Multiple configuration options, including:
 * Read:Write ratio
 * Random and sequential key name pattern policies
 * Random or ranged key expiration
 * Redis cluster
 * TLS support
 * ...and much more

Read more at:

* [A High Throughput Benchmarking Tool for Redis and Memcached](https://redislabs.com/blog/memtier_benchmark-a-high-throughput-benchmarking-tool-for-redis-memcached)
* [Pseudo-Random Data, Gaussian Access Pattern and Range Manipulation](https://redislabs.com/blog/new-in-memtier_benchmark-pseudo-random-data-gaussian-access-pattern-and-range-manipulation)

## Getting Started

### Prerequisites

The following libraries are required for building:

* libevent 2.0.10 or newer.
* libpcre 8.x.
* OpenSSL (unless TLS support is disabled by `./configure --disable-tls`).

The following tools are required
* autoconf
* automake
* pkg-config
* GNU make
* GCC C++ compiler

#### CentOS 6.x

On a CentOS 6.x system, use the following to install prerequisites:
```
# yum install autoconf automake make gcc-c++
# yum install pcre-devel zlib-devel libmemcached-devel libevent-devel openssl-devel
```

CentOS 6.4 ships with older versions of libevent, which must be manually built
and installed as follows:

To download, build and install libevent-2.0.21:
```
$ wget https://github.com/downloads/libevent/libevent/libevent-2.0.21-stable.tar.gz
$ tar xfz libevent-2.0.21-stable.tar.gz
$ pushd libevent-2.0.21-stable
$ ./configure
$ make
$ sudo make install
$ popd
```

The above steps will install into /usr/local so it does not confict with the 
distribution-bundled versions.  The last step is to set up the 
PKG_CONFIG_PATH so configure can find the newly installed library.

```
$ export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}
```

Then proceed to follow the build instructions below.

#### Ubuntu/Debian

On Ubuntu/Debian distributions, simply install all prerequisites as follows:

```
# apt-get install build-essential autoconf automake libpcre3-dev libevent-dev pkg-config zlib1g-dev libssl-dev
```

#### macOS

To build natively on macOS, use Homebrew to install the required dependencies:

```
$ brew install autoconf automake libtool libevent pkg-config openssl@1.1
```

When running `./configure`, if it fails to find libssl it may be necessary to
tweak the `PKG_CONFIG_PATH` environment variable:

```
PKG_CONFIG_PATH=/usr/local/opt/openssl@1.1/lib/pkgconfig ./configure
```

### Building and installing

After downloading the source tree, use standard autoconf/automake commands:

```
$ autoreconf -ivf
$ ./configure
$ make
$ sudo make install
```

#### Testing

The project includes a basic set of integration tests.


**Integration tests**


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

## Using Docker

Use available images on Docker Hub:

```
# latest stable release
$ docker run --rm redislabs/memtier_benchmark:latest --help

# master branch edge build
$ docker run --rm redislabs/memtier_benchmark:edge --help
```

Or, build locally:

```
$ docker build -t memtier_benchmark .
$ docker run --rm memtier_benchmark --help
```

### Using Docker Compose
```
$ docker-compose -f docker-compose.memcached.yml up --build
```
or
```
$ docker-compose -f docker-compose.redis.yml up --build
```

## Using memtier_benchmark

See the included manpage or run:

```
$ memtier_benchmark --help
```

for command line options.

### Cluster mode

#### Connections

When using the cluster-mode option, each client opens one connection for each node.
So, when using a large number of threads and clients, the user must verify that he is not
limited by the maximum number of file descriptors.

#### Using sequential key pattern

When there is an asymmetry between the Redis nodes and user set
the --requests option, there may be gaps in the generated keys.

Also, the ratio and the key generator is per client (and not connection).
In this case, setting the ratio to 1:1 does not guarantee 100% hits because
the keys spread to different connections/nodes.

### Full latency spectrum analysis

For distributions that are non-normal, such as the latency, many “basic rules” of normally distributed statistics are violated.  Instead of computing just the mean, which tries to express the whole distribution in a single result, we can use a sampling of the distribution at intervals -- percentiles, which tell you how many requests actually would experience that delay. 


When used for normally distributed data, the samples are usually taken at regular intervals. However, since the data does not obey to a normal distribution it would be very expensive to keep equally spaced intervals of latency records while enabling large value ranges. We can apply algorithms that can calculate a good approximation of percentiles at minimal CPU and memory cost, such as [t-digest](https://github.com/tdunning/t-digest) or [HdrHistogram](https://github.com/HdrHistogram/HdrHistogram_c). On memtier_benchmark we’ve decided to use the  HdrHistogram due to its low memory footprint, high precision, zero allocation during the benchmark and constant access time.


By default Memtier will output the 50th, 99th, and 99.9th percentiles. They are the latency thresholds at which 50%, 99%, and 99.9% of commands are faster than that particular presented value. 
To output different percentiles you should use the --print-percentiles option followed by the comma separated list of values ( example: `--print-percentiles 90,99,99.9,99.99` ).

#### Saving the full latency spectrum
To save the full latencies you should use the --hdr-file-prefix option followed by the prefix name you wish the filenames to have. 
Each distinct command will be saved into two different files - one in .txt (textual format) and another in .hgrm (HistogramLogProcessor format).
The textual format can be hard to analyze solely, but you can use an [online formatter](http://hdrhistogram.github.io/HdrHistogram/plotFiles.html) to generate visual histograms from it. The .hgrm format will be later added as input to Redislabs [mbdirector](https://github.com/redislabs/mbdirector) to enable visualization of time-domain results.

Sample Visual Feel of the full latency spectrum using an [online formatter](http://hdrhistogram.github.io/HdrHistogram/plotFiles.html):
![alt text][sample_visual_histogram]


[sample_visual_histogram]: ./docs/sample_visual_histogram.png "Sample Full Latency Spectrum Histogram"
