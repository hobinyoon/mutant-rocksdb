include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += "-DROCKSDB_JEMALLOC"
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

override CXXFLAGS += -O2 -std=c++11
	
.PHONY: clean librocksdb objs

all: quizup

.cpp.o:
	$(CXX) $(CXXFLAGS) $(PLATFORM_CXXFLAGS) -c $< -o $@ -I../include

objs = main.o conf.o cons.o util.o

quizup: librocksdb $(objs)
	$(CXX) $(CXXFLAGS) -I../include $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) -Wl,--as-needed -ldl -L.. $(objs) -o$@ \
		-lrocksdb \
		-lboost_filesystem \
		-lboost_program_options \
		-lboost_regex \
		-lboost_system \
		-lboost_timer \
		-lyaml-cpp

clean:
	rm -rf ./quizup *.o

librocksdb:
	cd .. && $(MAKE) shared_lib
