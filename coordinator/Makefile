CC=g++-4.7
INCLUDE_DIRS=-I.. -I.
CFLAGS=-O3 -std=c++0x -Wall -Wextra
LIBS=-lbusybee -le -lrt -pthread -ltcmalloc
CLIBS=

SRCS=central.cc
OBJS=central.o
TARGET=coordinator

all: depend $(TARGET)

debug:	CFLAGS += -g -O0
debug:	LIBS += -pg
debug:	CLIBS += -pg
debug:  all

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
