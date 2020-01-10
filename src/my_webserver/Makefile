#
# Copyright (c) 2013 No Face Press, LLC
# License http://opensource.org/licenses/mit-license.php MIT License
#

#This makefile is used to test the other Makefiles

# $$(PROG): $(CIVETWEB_LIB) $(SRC)
# 	$(CXX) -o $@ $(CFLAGS) $(LDFLAGS) $(SRC) $(CIVETWEB_LIB) $(LIBS)

TARGET_EXEC ?= server

BUILD_DIR ?= ./build
SRC_DIRS ?= .

THIRD_PARTY = ../../third_party

CIVETWEB_TOP := $(THIRD_PARTY)/civetweb/
CIVETWEB_LIB = $(BUILD_DIR)/libcivetweb.a

SRCS := $(shell find $(SRC_DIRS) -name '*.cpp' -or -name '*.c' -or -name '*.s')
OBJS := $(SRCS:%=$(BUILD_DIR)/%.o)
DEPS := $(OBJS:.o=.d)

INC_DIRS := $(shell find $(SRC_DIRS) -type d)
ADDITIONAL_INCLUDES = -I$(CIVETWEB_TOP)/include -I$(THIRD_PARTY)/json
INC_FLAGS := $(addprefix -I,$(INC_DIRS)) $(ADDITIONAL_INCLUDES)

CPPFLAGS ?= $(INC_FLAGS) -MMD -MP -std=c++17 -O3 -march=native -g -Wno-narrowing
CFLAGS ?= $(INC_FLAGS) -MMD -MP -O3 -march=native -g -Wno-narrowing


ifdef PRODUCTION
	CPPFLAGS += -DUSE_PRODUCTION_SETTINGS
	LINUX = -Wl,--no-as-needed
endif

LDFLAGS ?= -lstdc++ -lm -lpthread $(LINUX) -ldl


JEMALLOC_OPTS ?= -L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs`

$(BUILD_DIR)/$(TARGET_EXEC): $(OBJS) $(CIVETWEB_LIB)
	$(CC) $(OBJS) -o $@ $(LDFLAGS) $(CIVETWEB_LIB) $(JEMALLOC_OPTS)

# c++ source
$(BUILD_DIR)/%.cpp.o: %.cpp $(CIVETWEB_LIB)
	$(MKDIR_P) $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

# c source
$(BUILD_DIR)/%.c.o: %.c
	$(MKDIR_P) $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(CIVETWEB_LIB):
	$(MAKE) -C $(CIVETWEB_TOP) lib WITH_CPP=1
	$(MKDIR_P) $(BUILD_DIR)
	cp $(CIVETWEB_TOP)/libcivetweb.a $(BUILD_DIR)/


.PHONY: clean start

clean:
	$(RM) -r $(BUILD_DIR)

start:
	$(BUILD_DIR)/$(TARGET_EXEC) 8081 ../../data/

start-big:
	$(BUILD_DIR)/$(TARGET_EXEC) 8081 ../../data-big/

start-small:
	$(BUILD_DIR)/$(TARGET_EXEC) 8081 ../../data-small/

start-medium:
	$(BUILD_DIR)/$(TARGET_EXEC) 8081 ../../data-medium/

test:
	$(BUILD_DIR)/$(TARGET_EXEC)

-include $(DEPS)

MKDIR_P ?= mkdir -p

.PHONY: all clean start start-big
