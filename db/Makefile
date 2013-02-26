CC=g++-4.7
INCLUDE_DIRS=-I.. -I.
CFLAGS=-g -O0 -std=c++0x -Wall -Wextra
LIBS=-lbusybee-sta -le -lrt -pthread

OBJS=graph.o
TARGET=graph

$(TARGET):	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

%.o:	%.cc
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)
