CXX = g++
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra -pthread
INCLUDES = -Iinclude
LDFLAGS = -pthread
TARGET = aether-rate-limiter

SRC_DIR = src
TEST_DIR = tests
BENCH_DIR = benchmarks
BUILD_DIR = build

SOURCES = $(wildcard $(SRC_DIR)/*.cpp)
OBJECTS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SOURCES))

TEST_SOURCES = $(wildcard $(TEST_DIR)/*.cpp)
TEST_TARGET = $(BUILD_DIR)/unit_tests

BENCH_SOURCES = $(wildcard $(BENCH_DIR)/*.cpp)
BENCH_TARGET = $(BUILD_DIR)/benchmark

.PHONY: all build test clean benchmark directories

all: directories build test benchmark

directories:
	@mkdir -p $(BUILD_DIR)

build: directories $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

test: directories
	@if [ -n "$(TEST_SOURCES)" ]; then \
		$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_SOURCES) $(filter-out $(BUILD_DIR)/main.o, $(OBJECTS)) -o $(TEST_TARGET) $(LDFLAGS); \
		$(TEST_TARGET); \
	else \
		echo "No tests found in $(TEST_DIR)/, skipping."; \
	fi

benchmark: directories
	@if [ -n "$(BENCH_SOURCES)" ]; then \
		$(CXX) $(CXXFLAGS) $(INCLUDES) $(BENCH_SOURCES) $(filter-out $(BUILD_DIR)/main.o, $(OBJECTS)) -o $(BENCH_TARGET) $(LDFLAGS); \
		$(BENCH_TARGET); \
	else \
		echo "No benchmarks found in $(BENCH_DIR)/, skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)