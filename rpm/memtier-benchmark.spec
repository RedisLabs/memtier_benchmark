# RPM spec file for memtier_benchmark
# Copyright (C) 2011-2026 Redis Labs Ltd.

%define version_from_configure %(awk -F'[(),]' '/AC_INIT/ {gsub(/ /, "", $3); print $3}' %{_sourcedir}/../configure.ac 2>/dev/null || echo "0.0.0")

Name:           memtier-benchmark
Version:        %{version_from_configure}
Release:        1%{?dist}
Summary:        NoSQL Redis and Memcache traffic generation and benchmarking tool

License:        GPLv2
URL:            https://github.com/redis/memtier_benchmark
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  gcc-c++
BuildRequires:  make
BuildRequires:  autoconf
BuildRequires:  automake
BuildRequires:  libtool
BuildRequires:  libevent-devel
BuildRequires:  openssl-devel
BuildRequires:  zlib-devel
BuildRequires:  pkgconfig

%description
memtier_benchmark is a command line utility developed by Redis Labs
for load generation and benchmarking NoSQL key-value databases.

It offers the following features:
  * Support for both Redis and Memcache protocols (text and binary)
  * Multi-threaded multi-client execution
  * Multiple configuration options, including:
    - Read:Write ratio
    - Random and sequential key name pattern policies
    - Random or ranged key expiration
    - Redis cluster support
    - TLS connections support

%prep
%setup -q

%build
autoreconf -ivf
%configure
%make_build

%install
%make_install

%files
%license COPYING
%doc README.md
%{_bindir}/memtier_benchmark
%{_mandir}/man1/memtier_benchmark.1*

%changelog
* %(date "+%a %b %d %Y") Redis Team <oss@redis.com> - %{version}-%{release}
- See https://github.com/redis/memtier_benchmark/releases

