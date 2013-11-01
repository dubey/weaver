CC=g++-4.8
INCLUDE_DIRS=-I.. -I.
CFLAGS=-std=c++0x -Wall -Wextra
LIBS=-lbusybee -le -pthread -lrt -lchronos -lhyperdex-client
CLIBS=

SRCS=shard.cc
OBJS=shard.o
TARGET=shard

all: depend $(TARGET)

debug:	CFLAGS += -g
debug:  all

profile:	LIBS += -pg
profile:	CLIBS += -pg
profile:	debug

opt:	CFLAGS += -O2
opt:  all

depend: .depend

.depend: $(SRCS)
	rm -f ./.depend
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $^ -MF ./.depend;

$(TARGET):	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

%.o:	%.cc
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $< -D_GLIBCXX_USE_NANOSLEEP $(CLIBS)

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)

force:	clobber all

forcedebug:	clobber debug

include .depend
