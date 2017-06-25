
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG

TESTS = election_test follower_test candidate_test leader_test \
		replicate_test mt_election_test mt_app_test \
		raft_disk_test raft_disk_test_c raft_config_test tconfig_test

INCLS += -I../ 
INCLS += -I../build
INCLS += -I../cutils
INCLS += -I/usr/local/Cellar/protobuf/3.3.0/include
#INCLS += -I$(HOME)/open-src/github.com/microsoft/GSL/include
#INCLS += -I$(HOME)/project/include
#LINKS += -L$(HOME)/project/lib
LINKS += -L/usr/local/lib
LINKS += -lpthread -lprotobuf

GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h

CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)

#PROTOS_PATH = ../raftpb
#PROTOC = $(HOME)/project/bin/protoc
#
all: $(TESTS)

clean :
	rm -f $(TESTS) *.a *.o 

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

raft_test.a: tconfig_helper.o test_helper.o
	$(AR) $(ARFLAGS) $@ $^

replicate_test: replicate_test.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

mt_election_test: mt_election_test.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

mt_app_test: mt_app_test.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

raft_disk_test: raft_disk_test.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

raft_disk_test_c: raft_disk_test_c.o test_helper.o gtest_main.a ../libdraft.a
	$(BUILDEXE)

raft_config_test: raft_config_test.o raft_test.a gtest_main.a ../build/libd-raft.a
	$(BUILDEXE)

tconfig_test: tconfig_test.o tconfig_helper.o gtest_main.a ../build/libd-raft.a
	$(BUILDEXE)

leader_test: leader_test.o raft_test.a gtest_main.a ../build/libd-raft.a
	$(BUILDEXE)

election_test: election_test.o raft_test.a gtest_main.a ../build/libd-raft.a
	$(BUILDEXE)

candidate_test: candidate_test.o raft_test.a gtest_main.a ../build/libd-raft.a
	$(BUILDEXE)

follower_test: follower_test.o raft_test.a gtest_main.a ../build/libd-raft.a
	$(BUILDEXE)


#%.pb.cc: %.proto
#	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=../raftpb/ $<


%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

