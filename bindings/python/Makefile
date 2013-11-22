CC=gcc-4.8
CXX=g++-4.8
PY=python

SRCS=client.pyx setup.py ../../client/client.h ../../common/weaver_constants.h ../../db/element/remote_node.h ../../node_prog/*.h 
TARGET=client.so

all: $(TARGET)

$(TARGET): $(SRCS)
	CC=$(CC) CXX=$(CXX) $(PY) setup.py build_ext --inplace

clean:
	rm -f *.cpp
	rm -rf build/

clobber: clean
	rm -f $(TARGET)
