CFLAGS=-g -O0 -std=c++0x -D__STDC_LIMIT_MACROS
OBJS=message.o
LIBS=-L/usr/local/lib
INCLUDE_DIRS=-I../..

message:	$(OBJS)
	g++ -o $@ $(OBJS) $(LIBS) -lbusybee-sta -le -lrt

message.o:	message.cc
	g++ $(CFLAGS) $(INCLUDE_DIRS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f message
