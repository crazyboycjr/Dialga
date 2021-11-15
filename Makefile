.PHONY: clean compiledb

CXX ?= g++
CC ?= gcc

INCPATH := -Isrc -Iinclude
CFLAGS := -std=c++17 -msse2 -ggdb -Wall -finline-functions $(INCPATH) $(ADD_CFLAGS)
LDFLAGS := -libverbs -lglog -lpthread -lgflags $(ADD_LDFLAGS)

DEBUG := 0
ifeq ($(DEBUG), 1)
CFLAGS += -ftrapv -fstack-protector-strong
else
CFLAGS += -O3 -DNDEBUG
endif

ifdef ASAN
CFLAGS += -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls
endif

SRCS := $(shell find src -type f -name "*.cpp")
OBJS := $(patsubst src/%,build/%,$(SRCS:.cpp=.o))

all: dialga app

# The kvstore static library
dialga: build/libdialga.a

build/libdialga.a: $(OBJS)
	$(AR) crv $@ $(filter %.o, $?)

build/%.o: src/%.cpp
	@mkdir -p $(@D)
	$(CXX) $(INCPATH) $(CFLAGS) -MM -MT build/$*.o $< >build/$*.d
	$(CXX) $(INCPATH) $(CFLAGS) -c $< -o $@

compiledb:
	@[[ -f compile_commands.json ]] && mv -f compile_commands.json /tmp || true
	bear -- make

-include build/*.d
-include build/*/*.d

clean: clean_app
	$(RM) $(OBJS)
	$(RM) $(OBJS:.o=.d)
	$(RM) build/libdialga.a

-include app/Makefile
app: $(APPS)
