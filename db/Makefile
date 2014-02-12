CC=g++-4.8
INCLUDE_DIRS=-I.. -I.
CFLAGS=-std=c++0x -Wall -Wextra
LIBS=-lbusybee -le -pthread -lrt -lchronos -lhyperdex-client -lreplicant
CLIBS=

SRCS=../common/ids.cc ../common/serialization.cc ../common/server.cc ../common/configuration.cc ../common/server_manager_link.cc server_manager_link_wrapper.cc shard.cc
OBJS=ids.o serialization.o server.o configuration.o server_manager_link.o server_manager_link_wrapper.o shard.o
TARGET=shard

all: depend $(TARGET)

#gdwarf-2 flag necessary for gdb 7.4 with gcc4.8
debug:	CFLAGS += -g -gdwarf-2
debug:  all

profile:	LIBS += -pg
profile:	CLIBS += -pg
profile:	debug

opt:	CFLAGS += -O2 -DNDEBUG
opt:  all

depend: .depend

.depend: $(SRCS)
	rm -f ./.depend
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $(SRCS) -MF ./.depend;

$(TARGET):	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

$(OBJS):	$(SRCS)
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $(SRCS) -D_GLIBCXX_USE_NANOSLEEP $(CLIBS)

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)

force:	clobber all

forcedebug:	clobber debug

include .depend
