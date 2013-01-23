INCLUDE_DIRS=-I..
CFLAGS=-g -O0 -std=c++0x -D__STDC_LIMIT_MACROS
LIBS_DIR=-L/usr/local/lib
LIBS=-lbusybee-sta -le -lrt -pthread

OBJS=run.o
TARGET=tests

tests:	$(OBJS)
	g++ -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

%.o:	%.cc
	g++ $(CFLAGS) $(INCLUDE_DIRS) -c $<

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)

check:
	./tests
