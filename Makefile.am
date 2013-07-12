# Makefile for memtier_benchmark
# Copyright (C) 2011-2013 Garantia Data Ltd.

# This file is part of memtier_benchmark.

# memtier_benchmark is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

AUTOMAKE_OPTIONS = foreign 1.9

EXTRA_DIST = README.md COPYING

bin_PROGRAMS = memtier_benchmark

memtier_benchmark_CPPFLAGS = $(LIBEVENT_CFLAGS)
memtier_benchmark_SOURCES = \
	memtier_benchmark.cpp memtier_benchmark.h \
	client.cpp client.h \
	protocol.cpp protocol.h \
	obj_gen.cpp obj_gen.h \
	item.cpp item.h \
	file_io.cpp file_io.h \
	config_types.cpp config_types.h
memtier_benchmark_LDADD = $(LIBEVENT_LIBS)

dist_man1_MANS = memtier_benchmark.1

.PHONY: rebuild-man
rebuild-man:
	help2man --name="NoSQL benchmark tool" --no-info --no-discard-stderr --output=memtier_benchmark.1 $(builddir)/memtier_benchmark
