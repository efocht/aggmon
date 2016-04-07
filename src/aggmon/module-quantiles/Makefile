NAME = quantiles
SRCS = $(wildcard *.cpp)
OBJS = $(SRCS:%.cpp=%.o)
SONAME = $(NAME).so
PYVER=$(shell python2 -c 'import sys, re; print re.match("(\d+.\d+).\d+", sys.version).group(1)')
PYINCDIR = $(shell /usr/bin/python$(PYVER)-config --includes) -I/usr/include/boost
PYLIBDIR = $(shell /usr/bin/python$(PYVER)-config --exec-prefix)/lib
LINKLIBS = $(shell /usr/bin/python$(PYVER)-config --ldflags) -L/usr/lib/x86_64-linux-gnu/ -lboost_python -ldl
CFLAGS = -I./
#CFLAGS += -H -DDEBUG -ggdb
CFLAGS += -fPIC -O2
#CFLAGS += -std=c++11
LDFLAGS = -shared -lc -Wl,-soname,$(SONAME),-rpath,$(PYLIBDIR)
#LDFLAGS += -ggdb
CC = g++
LD = gcc

all: $(SONAME) $(OBJS) Makefile

$(SONAME): $(OBJS) Makefile
	$(LD) $(LDFLAGS) $(OBJS) -o $(SONAME) $(LINKLIBS)
	ldd -r $(SONAME) | grep undefined || :

%.o: %.cpp
	$(CC) -c $(PYINCDIR) $(CFLAGS) $< -o $@

clean:
	rm -f *.o *.so *.pyc $(SONAME)

check:
	@for L in gcc gcc-c++ python-devel boost-python; do \
	    if ! rpm -qa | grep -q $$L; then \
	        echo "Package $$L missing!"; \
	    fi; \
	done

test: run
run: $(SONAME)
	@PYTHONPATH=$$PYTHONPATH:. python test.py

.PHONY: all clean

