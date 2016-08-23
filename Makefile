
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra # -D TEST_DEBUG

TESTS = utils_test

INCLS += -I./raftpb/
INCLS += -I../cutils/
INCLS += -I$(HOME)/project/include
LINKS += -L$(HOME)/project/lib
LINKS += -lpthread -lprotobuf

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

PROTOS_PATH = raftpb
PROTOC = $(HOME)/project/bin/protoc
#GRPC_CPP_PLUGIN = grpc_cpp_plugin
#GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o raftpb/*.o raftpb/raft.pb.* test/*.o libdraft.a

#raft.o raft_impl.o replicate_tracker.o raft_config.o craftpb/raft.pb.o 
libdraft.a: raft_mem.o
	$(ARSTATICLIB)

%.pb.cc: raftpb/%.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=raftpb/ $<

%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

