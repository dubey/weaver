CC=g++-4.7
INCLUDE_DIRS=-I.. -I../db
CFLAGS=-g -O0 -std=c++0x -D__STDC_LIMIT_MACROS
LIBS=-lbusybee -le -lrt -pthread

SRCS=run.cc
OBJS=run.o
TARGET=tests.bin

all: depend $(TARGET)

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

include .depend
