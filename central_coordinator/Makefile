CFLAGS=-g -O0 -std=c++0x
OBJS=central.o
LIBS=-L/usr/local/lib

central:	$(OBJS)
	g++ -o $@ $(OBJS) $(LIBS) -lbusybee-sta -le -lrt -pthread

central.o:	central.cc
	g++ $(CFLAGS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f central
