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

# timestamper
bin_PROGRAMS=		timestamper
timestamper_SOURCES=../common/comm_wrapper.cc \
                    ../common/configuration.cc \
                    ../common/server.cc \
                    ../common/ids.cc \
                    ../common/serialization.cc \
					timestamper.cc
timestamper_CFLAGS= $(AM_CFLAGS)
timestamper_CXXFLAGS= $(AM_CXXFLAGS)

# server manager so
lib_LTLIBRARIES=			libservermanager.la
libservermanager_la_SOURCES=server_barrier.cc \
							server_manager.cc \
							transitions.cc \
							symtable.c \
							../common/server.cc \
							../common/ids.cc \
							../common/serialization.cc
