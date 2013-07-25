CC=g++-4.7
INCLUDE_DIRS=-I.. -I.
CFLAGS=-O2 -std=c++0x -Wall -Wextra
LIBS=-lbusybee -le -pthread -lrt

COORD_SRC=reqhandler.cc
STATE_SRC=state_manager.cc
SRCS=$(STATE_SRC) $(COORD_SRC)
COORD_OBJ=reqhandler.o
STATE_OBJ=state_manager.o
OBJS=$(STATE_OBJ) $(COORD_OBJ)
COORD_TARGET=reqhandler
STATE_TARGET=state_manager
TARGETS=$(STATE_TARGET) $(COORD_TARGET)

all: depend $(TARGETS)

debug:	CFLAGS += -g -O0
debug:  all

profile:	LIBS += -pg
profile:	CLIBS += -pg
profile:	debug

depend: .depend

.depend: $(SRCS)
	rm -f ./.depend
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $(COORD_SRC) -MF ./.depend;
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $(STATE_SRC) -MF ./.depend;

$(COORD_TARGET):	$(COORD_OBJ)
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -o $@ $(COORD_OBJ) $(LIBS)

$(STATE_TARGET):	$(STATE_OBJ)
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -o $@ $(STATE_OBJ) $(LIBS)

%.o:	%.cc
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $< -D_GLIBCXX_USE_NANOSLEEP -D__STDC_LIMIT_MACROS $(CLIBS)

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGETS)

force:	clobber all

forcedebug:	clobber debug

include .depend
