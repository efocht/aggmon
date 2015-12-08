PYLIBDIR = $(shell python-config --exec-prefix)/lib
LIBS = -L$(PYLIBDIR) $(shell python-config --libs) -lboost_python
INC = $(shell python-config --include)
CFLAGS = -O2 -fPIC
CFLAGS += -std=c++11
LDFLAGS = -Wl,-rpath,$(PYLIBDIR) -shared
CC = g++
LD = gcc

all: module.so Makefile

module.so: module.o Makefile
	$(LD) $(LDFLAGS) $(LIBS) $< -o $@

module.o: module.cpp Makefile
	$(CC) -c $(INC) $(CFLAGS) $< -o $@

test: run
run: module.so
	@PYTHONPATH=.:$$PYTHONPATH python module.py

check:
	@for L in gcc gcc-c++ python-devel boost_python; do \
	    if ! rpm -qa | grep -q $$L; then \
	        echo "Package $$L missing!"; \
	    fi; \
	done

help:
	@cat README

clean:
	rm -rf *.so *.o *.pyc

.PHONY: all clean

