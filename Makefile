CC      = gcc
CFLAGS ?= -O1 -g -Wall -Wextra -pthread
LDFLAGS = -pthread -rdynamic -ldl
INC     = -Iinclude -Isrc

SRC  = $(wildcard src/*.c)
OBJ  = $(SRC:.c=.o)

all: dsm_node dsm_query tools/dsm_lab apps/life/life.so

dsm_node: $(OBJ)
	$(CC) $(OBJ) -o $@ $(LDFLAGS)

src/%.o: src/%.c include/*.h src/*.h
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

dsm_query: tools/dsm_query.c
	$(CC) $(CFLAGS) $< -o $@

tools/dsm_lab: tools/dsm_lab.c
	$(CC) $(CFLAGS) $< -o $@

apps/life/life.so: apps/life/life.c apps/life/life_common.h
	$(CC) $(CFLAGS) -shared -fPIC $(INC) -o $@ apps/life/life.c

# TODO para Integrante B:
# dsm_run: tools/dsm_run.c
# 	$(CC) $(CFLAGS) $(INC) $< -o $@ $(LDFLAGS)
# apps/filestore/filestore.so: apps/filestore/filestore.c
# 	$(CC) $(CFLAGS) -shared -fPIC $(INC) -o $@ apps/filestore/filestore.c

tests: all
	bash tests/run_local.sh

asan: CFLAGS += -fsanitize=address
asan: LDFLAGS += -fsanitize=address
asan: clean all

clean:
	rm -f src/*.o dsm_node dsm_query tools/dsm_lab apps/life/life.so

.PHONY: all asan clean tests
