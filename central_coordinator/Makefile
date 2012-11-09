CFLAGS=-g -O0
OBJS=central.o
LIBS=-L/usr/local/lib

central:	$(OBJS)
	g++ -o $@ $(OBJS) $(LIBS) -lbusybee-sta -le -lrt

central.o:	central.cc
	g++ $(CFLAGS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f central
