CC=g++-4.7
INCLUDE_DIRS=-I.. -I. -I../db
CFLAGS=-g -O0 -std=c++0x -Wall -Wextra
LIBS=-lbusybee-sta -le -lrt -pthread

OBJS=central.o
TARGET=coordinator

$(TARGET):	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

%.o:	%.cc
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)
