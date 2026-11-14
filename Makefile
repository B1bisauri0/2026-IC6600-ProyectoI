CC      = gcc
CFLAGS ?= -O1 -g -Wall -Wextra -pthread
LDFLAGS = -pthread -rdynamic -ldl
INC     = -Iinclude -Isrc

SRC  = $(wildcard src/*.c)
OBJ  = $(SRC:.c=.o)

all: dsm_node dsm_query tools/dsm_lab

dsm_node: $(OBJ)
	$(CC) $(OBJ) -o $@ $(LDFLAGS)

src/%.o: src/%.c include/*.h src/*.h
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

dsm_query: tools/dsm_query.c
	$(CC) $(CFLAGS) $< -o $@

tools/dsm_lab: tools/dsm_lab.c
	$(CC) $(CFLAGS) $< -o $@

asan: CFLAGS += -fsanitize=address
asan: LDFLAGS += -fsanitize=address
asan: clean all

clean:
	rm -f src/*.o dsm_node dsm_query tools/dsm_lab

.PHONY: all asan clean
