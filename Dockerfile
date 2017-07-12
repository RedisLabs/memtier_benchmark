FROM ubuntu:16.04
LABEL Description="memtier_benchmark"

RUN apt-get update
RUN apt-get install -yy build-essential autoconf automake libpcre3-dev libevent-dev pkg-config zlib1g-dev git libboost-all-dev cmake flex
RUN git clone https://github.com/RedisLabs/memtier_benchmark.git
WORKDIR /memtier_benchmark
RUN autoreconf -ivf && ./configure && make && make install
ENTRYPOINT ["memtier_benchmark"]
