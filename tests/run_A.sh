#!/bin/bash
# Pruebas del modulo A: T3, T4, T5, asignacion grande, nodo caido y Juego de la Vida contra la referencia.
cd "$(dirname "$0")" || exit 1
make -s || exit 1
P=../apps/life/patterns
fail=0
ok()  { echo "OK     $*"; }
bad() { echo "FALLA  $*"; fail=1; }

if timeout 60 ./test_unit_A >/tmp/unit.out 2>&1; then ok "test_unit_A (clock, enomem, pagesize, stats)"; else bad "test_unit_A"; cat /tmp/unit.out; fi

for spec in "t4 2" "t4 3" "t3 2" "t3 3" "big 2" "big 3" "down 3" "t7 3"; do
  if timeout 120 ./test_cluster $spec >/tmp/t.out 2>&1; then ok "$spec"; else bad "$spec"; tail -5 /tmp/t.out; fi
done
if CACHE=16 timeout 120 ./test_cluster t5 2 >/tmp/t.out 2>&1; then ok "t5 2 (cache=16, fifo)"; else bad "t5 2"; tail -5 /tmp/t.out; fi
if CACHE=16 POLICY=clock timeout 120 ./test_cluster t5 2 >/tmp/t.out 2>&1; then ok "t5 2 (cache=16, clock)"; else bad "t5 2 (clock)"; tail -5 /tmp/t.out; fi
if CACHE=2 POLICY=fifo timeout 60 ./test_cluster clock_diff 2 >/tmp/t.out 2>&1; then ok "clock_diff (cache=2, fifo: desaloja P0)"; else bad "clock_diff fifo"; tail -5 /tmp/t.out; fi
if CACHE=2 POLICY=clock timeout 60 ./test_cluster clock_diff 2 >/tmp/t.out 2>&1; then ok "clock_diff (cache=2, clock: segunda oportunidad a P0, desaloja P1)"; else bad "clock_diff clock"; tail -5 /tmp/t.out; fi

if timeout 60 ./test_replica t11 >/tmp/repl_t11.out 2>&1; then ok "t11 (replica checkpoint, recuperacion y checksum)"; else bad "t11 (replica)"; cat /tmp/repl_t11.out; fi
if timeout 120 ./test_replica t12 >/tmp/repl_t12.out 2>&1; then ok "t12 (life --ft caida, reanudacion y hash life_ref)"; else bad "t12 (life --ft)"; cat /tmp/repl_t12.out; fi
if timeout 60 ./test_replica second_down >/tmp/repl_sec.out 2>&1; then ok "second_down (segunda caida durante recuperacion controlada)"; else bad "second_down"; cat /tmp/repl_sec.out; fi

life() {  # nodos n gens opciones...
  local nodes=$1 n=$2 gens=$3; shift 3
  local refopts=() skip=0
  for a in "$@"; do
    if [ $skip -eq 1 ]; then skip=0; continue; fi
    case "$a" in --threads) skip=1;; --solo|--quiet) ;; *) refopts+=("$a");; esac
  done
  ./life_ref $n $gens "${refopts[@]}" > /tmp/ref.txt
  if timeout 180 ./test_cluster life $nodes $n $gens "$@" > /tmp/dsm.txt 2>/tmp/err.txt && diff -q /tmp/ref.txt /tmp/dsm.txt >/dev/null; then
    ok "life nodos=$nodes n=$n gens=$gens $*"
  else bad "life nodos=$nodes n=$n gens=$gens $*"; tail -3 /tmp/err.txt; fi
}
life 1 64 15 --random 7
life 2 64 15 --random 7 --threads 2
life 3 64 15 --random 7 --threads 2
life 3 100 10 --random 99
life 2 70 40 --rle $P/rpentomino.rle
life 3 90 60 --rle $P/gosper_gun.rle --threads 3
life 3 64 10 --random 5 --solo --threads 2
exit $fail
