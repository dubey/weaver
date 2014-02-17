CC=gcc-4.8
CXX=g++-4.8
AM_CFLAGS=-std=c++0x -Wall -Wextra
AM_CXXFLAGS=-std=c++0x -Wall -Wextra
AM_CPPFLAGS=-I. -I..
LIBS=-lbusybee -le -pthread -lrt -lhyperdex-client -lchronos -lreplicant

if DEBUG
    AM_CFLAGS += -g3 -gdwarf-2 -O0
    AM_CXXFLAGS += -g3 -gdwarf-2 -O0
else
    AM_CFLAGS += -O2
    AM_CXXFLAGS += -O2
endif

# shard
bin_PROGRAMS=	shard
shard_SOURCES=	../common/ids.cc \
				../common/serialization.cc \
				../common/server.cc \
				../common/configuration.cc \
				../common/server_manager_link.cc \
                ../common/comm_wrapper.cc \
				server_manager_link_wrapper.cc \
				shard.cc

