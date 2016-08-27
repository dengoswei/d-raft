
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG


TESTS = election_test follower_test candidate_test

INCLS += -I../ -I../raftpb 
INCLS += -I../../cutils
#INCLS += -I$(HOME)/open-src/github.com/microsoft/GSL/include
INCLS += -I$(HOME)/project/include
LINKS += -L$(HOME)/project/lib
LINKS += -lpthread -lprotobuf

GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h

CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)

PROTOS_PATH = ../raftpb
PROTOC = $(HOME)/project/bin/protoc

all: $(TESTS)

clean :
	rm -f $(TESTS) gtest.a gtest_main.a *.o 

# Builds gtest.a and gtest_main.a.

# Usually you shouldn't tweak such internal variables, indicated by a
# trailing _.
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

# For simplicity and to avoid depending on Google Test's
# implementation details, the dependencies specified below are
# conservative and not optimized.  This is fine as Google Test
# compiles fast and for ordinary users its source rarely changes.
gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc

gtest_main.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc

gtest.a : gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

election_test: election_test.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

follower_test: follower_test.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

candidate_test: candidate_test.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

leader_test: leader_test.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

replicate_test: replicate_test.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=../raftpb/ $<


%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

