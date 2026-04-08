CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE -Isrc
SRC_DIR = src
BUILD_DIR = build
TEST_DIR = tests
TARGET = sql_processor

SRCS = $(wildcard $(SRC_DIR)/*.c)
APP_OBJS = $(SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)
LIB_SRCS = $(filter-out $(SRC_DIR)/main.c,$(SRCS))
LIB_OBJS = $(LIB_SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)
TEST_SRCS = $(wildcard $(TEST_DIR)/test_*.c)
TEST_BINS = $(TEST_SRCS:$(TEST_DIR)/%.c=$(BUILD_DIR)/tests/%)

all: $(TARGET)

$(TARGET): $(APP_OBJS)
	$(CC) $(CFLAGS) -o $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/tests/%: $(TEST_DIR)/%.c $(LIB_OBJS) | $(BUILD_DIR)/tests
	$(CC) $(CFLAGS) -o $@ $< $(LIB_OBJS)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/tests:
	mkdir -p $(BUILD_DIR)/tests

tests: $(TEST_BINS)

test: all tests
	./tests/run_tests.sh

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

.PHONY: all clean tests test
