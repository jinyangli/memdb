CXX=g++
CXXFLAGS += -std=c++0x -I. -g
#CXXFLAGS += -I. -g
LDFLAGS = 
LIBS += -lrt

TESTS = memdb_test
PROGRAMS = $(TESTS)

SOURCES = db/memtable.cc db/tableschema.cc
LIBOBJECTS = $(SOURCES:.cc=.o)
TESTHARNESS = ./util/testharness.o 

memdb_test : db/memdb_test.o $(TESTHARNESS) $(LIBOBJECTS)
	$(CXX) $(LDFLAGS) $< $(LIBOBJECTS) $(TESTHARNESS) -o $@ $(LIBS)

all: $(LIBOBJECTS) $(TESTS)

check: all $(PROGRAMS) $(TESTS)
	for t in $(TESTS); do echo "***** Running $$t"; ./$$t || exit 1; done

$(LIBRARY): $(LIBOBJECTS)
	rm -f $@
	$(AR) -rs $@ $(LIBOBJECTS)	

clean:
	rm -f $(PROGRAMS) $(TESTS) $(LIBRARY) */*.o

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@
