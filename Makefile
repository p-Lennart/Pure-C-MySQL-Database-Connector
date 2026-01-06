CC      ?= gcc
SANITIZE ?= none

CFLAGS  := -Wall -Werror -std=c17 -g -pthread
LDFLAGS := -pthread
MYSQL   := $(shell mysql_config --cflags --libs)

ifeq ($(SANITIZE),asan)
  ifeq ($(origin CC),default)
    CC := clang
  endif
  CFLAGS  += -fsanitize=address -fno-omit-frame-pointer -O1
  LDFLAGS += -fsanitize=address
endif

SRCS    := $(wildcard src/*.c)
TARGET  := bin/main

all: $(TARGET)

$(TARGET): $(SRCS) | bin
	$(CC) $(CFLAGS) $(SRCS) -o $(TARGET) $(LDFLAGS) $(MYSQL)

bin:
	mkdir -p bin

.PHONY: clean
clean:
	rm -f $(TARGET)
