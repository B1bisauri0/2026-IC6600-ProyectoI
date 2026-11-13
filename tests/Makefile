# Pruebas del modulo A (usa los sustitutos de tests/stub_b.c; no necesita al modulo B)
CC      ?= gcc
CFLAGS  ?= -O1 -g -Wall -Wextra -Werror -pedantic -std=gnu11 -pthread
SRC      = ../src/dsm.c ../src/pager.c ../src/directory.c ../src/stats.c stub_b.c test_cluster.c ../apps/life/life.c
SRC_UNIT = ../src/dsm.c ../src/pager.c ../src/directory.c ../src/stats.c stub_b.c test_unit_A.c ../apps/life/life.c

all: test_cluster test_unit_A life_ref

test_cluster: $(SRC) $(wildcard ../src/*.h ../include/*.h ../apps/life/*.h)
	$(CC) $(CFLAGS) -I../include -o $@ $(SRC)

test_unit_A: $(SRC_UNIT) $(wildcard ../src/*.h ../include/*.h ../apps/life/*.h)
	$(CC) $(CFLAGS) -I../include -o $@ $(SRC_UNIT)

life_ref: ../apps/life/life_ref.c ../apps/life/life_common.h
	$(CC) $(CFLAGS) -o $@ ../apps/life/life_ref.c

clean:
	rm -f test_cluster test_unit_A life_ref
