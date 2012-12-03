CFLAGS=-g -O0 -std=c++0x
OBJS=graph.o
LIBS=-L/usr/local/lib

graph:	$(OBJS)
	g++ -o $@ $(OBJS) $(LIBS) -lbusybee-sta -le -lrt

graph.o:	graph.cc
	g++ $(CFLAGS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f graph
