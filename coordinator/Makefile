CC=g++-4.8
INCLUDE_DIRS=-I.. -I.
CFLAGS=-std=c++0x #-Wall -Wextra
LIBS=-lbusybee -le -pthread -lrt -lhyperdex-client -lchronos
CLIBS=

SRCS=timestamper.cc
OBJS=timestamper.o
TARGET=timestamper

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
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $^ -MF ./.depend;

$(TARGET):	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

%.o:	%.cc
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $< -D_GLIBCXX_USE_NANOSLEEP -D__STDC_LIMIT_MACROS $(CLIBS)

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)

force:	clobber all

forcedebug:	clobber debug

include .depend
