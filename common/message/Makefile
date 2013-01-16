CFLAGS=-g -O0 -std=c++0x -D__STDC_LIMIT_MACROS
OBJS=message.o
LIBS=-L/usr/local/lib

message:	$(OBJS)
	g++ -o $@ $(OBJS) $(LIBS) -lbusybee-sta -le -lrt

message.o:	message.cc
	g++ $(CFLAGS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f message
