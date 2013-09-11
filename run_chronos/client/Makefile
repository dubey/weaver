CC=g++-4.7
INCLUDE_DIRS=-I.. -I.
LIBS=-lchronos
CLIBS=

TEST=weaver_client
SRCS=$(TEST).cc
OBJS=$(TEST).o
TARGET=$(TEST)

all: depend $(TARGET)

debug:	CFLAGS += -g -O0
debug:  all

profile:	LIBS += -pg
profile:	CLIBS += -pg
profile:	debug

depend: .depend

.depend: $(SRCS)
	rm -f ./.depend
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -MM $^ -MF ./.depend;

$(TARGET):	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS_DIR) $(LIBS)

%.o:	%.cc
	$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $< $(CLIBS)

clean:
	rm -f *.o

clobber:	clean
	rm -f $(TARGET)

force:	clobber all

forcedebug:	clobber debug

include .depend
