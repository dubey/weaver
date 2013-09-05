CC=g++-4.7
INCLUDE_DIRS=-I.. -I..
CFLAGS=-O2 -std=c++0x -Wall -Wextra -D__STDC_LIMIT_MACROS
LIBS=-lbusybee -le -pthread -lrt -lhyperclient

SRCS=run.cc
OBJS=run.o
TARGET=tests

all: depend $(TARGET)

debug:	CFLAGS += -g -O0
debug:  all

depend: .depend

.depend: $(SRCS)
	rm -f ./.depend
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $^ -MF ./.depend;

$(TARGET):	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

%.o:	%.cc
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $< -D_GLIBCXX_USE_NANOSLEEP

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)

check:	tests
	./tests

force:	clobber all

forcedebug:	clobber debug

include .depend
