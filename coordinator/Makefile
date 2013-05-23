CC=g++-4.7
INCLUDE_DIRS=-I.. -I.
CFLAGS=-g -O0 -std=c++0x #-Wall -Wextra
LIBS=-lbusybee -le -lrt -pthread

SRCS=central.cc
OBJS=central.o
TARGET=coordinator

all: depend $(TARGET)

depend: .depend

.depend: $(SRCS)
	rm -f ./.depend
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $^ -MF ./.depend;

$(TARGET):	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

%.o:	%.cc
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)

include .depend
