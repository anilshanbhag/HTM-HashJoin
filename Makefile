run_cmd=
PROG=AtomicHashBuild
ifneq (,$(shell which icc 2>/dev/null))
CXX=icc
endif # icc

ifeq ($(shell uname), Linux)
ifeq ($(target), android)
LIBS+= --sysroot=$(SYSROOT)
run_cmd=../../common/android.linux.launcher.sh
else
LIBS+= -lrt 
endif
else ifeq ($(shell uname), Darwin)
override CXXFLAGS += -std=c++14 -Wl,-rpath,$(TBBROOT)/lib
endif

all:	release

release: AtomicHashBuild.cpp
	$(CXX) -O2 -DNDEBUG -D_CONSOLE $(CXXFLAGS) -o bin/$(PROG) $^ -ltbb $(LIBS)

debug: AtomicHashBuild.cpp
	$(CXX) -O0 -D_CONSOLE -g -DTBB_USE_DEBUG $(CXXFLAGS) -o bin/$(PROG) $^ -ltbb_debug $(LIBS)

clean:
	rm -f $(PROG) *.o *.d

