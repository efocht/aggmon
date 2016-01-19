NAME = quantiles
SRCS = $(wildcard *.cpp)
OBJS = $(SRCS:%.cpp=%.o)
SONAME = $(NAME).so
PYINCDIR = $(shell python-config --include)
PYLIBDIR = $(shell python-config --exec-prefix)/lib
LINKLIBS = -L$(PYLIBDIR) $(shell python-config --libs) -lboost_python -ldl
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
	$(LD) $(LDFLAGS) -o $(SONAME) $(LINKLIBS) $(OBJS)
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

