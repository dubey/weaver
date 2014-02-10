CC=gcc-4.8
CXX=g++-4.8
INCLUDE_DIRS=-I.. -I.
CFLAGS=-std=c++0x -Wall -Wextra
REPLICANT_CFLAGS=-Wall -Wextra -fPIC
REPLICANT_SOFLAGS = -shared -Wl,-soname,
LIBS=-lbusybee -le -pthread -lrt -lhyperdex-client -lchronos
CLIBS=

SRCS=timestamper.cc
OBJS=timestamper.o
TARGET=timestamper

REPLICANT_SRCS=server_barrier.cc server_manager.cc transitions.cc symtable.c ../common/server.cc ../common/ids.cc ../common/serialization.cc
REPLICANT_OBJS=server_barrier.o server_manager.o symtable.o transitions.o server.o ids.o serialization.o
REPLICANT_TARGET=libwsrvrmgr.so

all: depend $(TARGET) $(REPLICANT_TARGET)

#gdwarf-2 flag necessary for gdb 7.4 with gcc4.8
debug:	CFLAGS += -g -gdwarf-2
debug:  all

profile:	LIBS += -pg
profile:	CLIBS += -pg
profile:	debug

opt:	CFLAGS += -O2 -DNDEBUG
opt:  all

depend: .depend

.depend: $(SRCS) $(REPLICANT_SRCS)
	rm -f ./.depend
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $^ -MF ./.depend;

$(TARGET):	$(OBJS)
	$(CXX) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

$(REPLICANT_TARGET):	$(REPLICANT_OBJS)
	$(CC) $(REPLICANT_SOFLAGS)$@ -o $@ $(REPLICANT_OBJS)

$(OBJS):	$(SRCS)
	$(CXX) $(CFLAGS) $(INCLUDE_DIRS) -c $< -D_GLIBCXX_USE_NANOSLEEP -D__STDC_LIMIT_MACROS $(CLIBS)

$(REPLICANT_OBJS):	$(REPLICANT_SRCS)
	$(CC) $(REPLICANT_CFLAGS) $(INCLUDE_DIRS) -c $(REPLICANT_SRCS)

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)
	rm -f $(REPLICANT_TARGET)

force:	clobber all

forcedebug:	clobber debug

include .depend
