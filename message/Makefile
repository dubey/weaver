CFLAGS=-g -O0
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
