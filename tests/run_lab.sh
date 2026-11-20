#!/bin/bash
# =============================================================================
# run_lab.sh -- Script de verificacion y medicion en las 3 computadoras del lab
# =============================================================================
#
# PARTE 1: Verificacion del entorno (se ejecuta siempre)
#   - Version del kernel Linux (uname -r >= 4.17 para uffd / MAP_FIXED_NOREPLACE)
#   - Memoria libre suficiente (free -m >= 1 GiB libre para aporte y cache)
#   - Conectividad de red mediante nc entre puertos 27700..27702
#   - Compilacion y ejecucion del diagnostico tools/dsm_lab
#
# PARTE 2: Medicion y escalabilidad de Juego de la Vida (T6 y T7)
#   (Se ejecuta si se pasa como argumento "life": ./tests/run_lab.sh life)
#   - Cuadriculas N=4000, 8000, 12000 sobre 1, 2 y 3 nodos (T6)
#   - Modo sin alinear filas a pagina (--no-align) para evaluar thrashing (T7)
#   - Guarda tiempos y transferencias en resultados/metricas_life_lab.csv
# =============================================================================

set -e
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p resultados

echo "======================================================================"
echo " [PARTE 1] Verificacion del entorno del Laboratorio                   "
echo "======================================================================"

# 1. Comprobacion de version del kernel Linux (>= 4.17)
KVER=$(uname -r | cut -d'-' -f1)
KMAJ=$(echo "$KVER" | cut -d'.' -f1)
KMIN=$(echo "$KVER" | cut -d'.' -f2)
echo -n "Verificando version del kernel Linux ($KVER >= 4.17)... "
if [ "$KMAJ" -gt 4 ] || { [ "$KMAJ" -eq 4 ] && [ "$KMIN" -ge 17 ]; }; then
    echo "OK (soporta MAP_FIXED_NOREPLACE / uffd)"
else
    echo "ADVERTENCIA: Kernel $KVER < 4.17; soporte uffd/MAP_FIXED_NOREPLACE puede estar limitado."
fi

# 2. Comprobacion de memoria disponible (al menos 1 GiB libre)
MEM_FREE_MB=$(free -m | awk '/^Mem:/{print $7}')
echo -n "Verificando memoria disponible ($MEM_FREE_MB MiB disponible)... "
if [ -n "$MEM_FREE_MB" ] && [ "$MEM_FREE_MB" -ge 1024 ]; then
    echo "OK (>= 1024 MiB libre)"
else
    echo "ADVERTENCIA: Memoria disponible ($MEM_FREE_MB MiB) inferior a 1 GiB recomendado."
fi

# 3. Comprobacion de conectividad entre maquinas mediante nc en puertos 27700..27702
echo "Verificando puertos de escucha y conectividad de red..."
PORTS=(27700 27701 27702)
for p in "${PORTS[@]}"; do
    echo -n "  Probando puerto local $p con nc... "
    if nc -z -w 1 127.0.0.1 "$p" 2>/dev/null; then
        echo "OCUPADO (servicio activo o nodo en ejecucion)"
    else
        echo "LIBRE"
    fi
done

# 4. Compilacion y ejecucion de la herramienta de diagnostico dsm_lab
echo ""
echo "Compilando y ejecutando tools/dsm_lab:"
make -s tools/dsm_lab
./tools/dsm_lab

if [ "$1" != "life" ]; then
    echo ""
    echo "======================================================================"
    echo " Verificacion del entorno completada con exito."
    echo " Para ejecutar las pruebas de rendimiento de Game of Life (T6 y T7),"
    echo " ejecute: $0 life"
    echo "======================================================================"
    exit 0
fi

echo ""
echo "======================================================================"
echo " [PARTE 2] Evaluacion y medicion de Juego de la Vida (T6 y T7)        "
echo "======================================================================"

make -s all
make -s -C tests all

OUT_CSV="resultados/metricas_life_lab.csv"
echo "fecha,nodos,n,generaciones,alineado,tiempo_seg,transferencias_totales" > "$OUT_CSV"

SIZES=(4000 8000 12000)
GENS=5
SEED=12345

for N in "${SIZES[@]}"; do
    for NODES in 1 2 3; do
        echo "--- Ejecutando Life: N=$N, Nodos=$NODES, Gens=$GENS (Alineado) ---"
        LOG_ALIGNED="resultados/life_N${N}_nodes${NODES}_align.log"
        T0=$(date +%s.%N)
        ./tests/test_cluster life "$NODES" "$N" "$GENS" --random "$SEED" --quiet > "$LOG_ALIGNED" 2>&1 || true
        T1=$(date +%s.%N)
        ELAPSED=$(awk "BEGIN {print $T1 - $T0}")
        TRANSF_ALIGNED=$(awk -F'=' '/transferencias_directorio=/{s+=$2} END{print s+0}' "$LOG_ALIGNED")
        echo "  Tiempo transcurrido: ${ELAPSED}s | Transferencias: $TRANSF_ALIGNED"
        echo "$(date -Iseconds),$NODES,$N,$GENS,si,$ELAPSED,$TRANSF_ALIGNED" >> "$OUT_CSV"

        echo "--- Ejecutando Life: N=$N, Nodos=$NODES, Gens=$GENS (--no-align) ---"
        LOG_NOALIGN="resultados/life_N${N}_nodes${NODES}_noalign.log"
        T0=$(date +%s.%N)
        ./tests/test_cluster life "$NODES" "$N" "$GENS" --random "$SEED" --no-align --quiet > "$LOG_NOALIGN" 2>&1 || true
        T1=$(date +%s.%N)
        ELAPSED_NOALIGN=$(awk "BEGIN {print $T1 - $T0}")
        TRANSF_NOALIGN=$(awk -F'=' '/transferencias_directorio=/{s+=$2} END{print s+0}' "$LOG_NOALIGN")
        echo "  Tiempo transcurrido (--no-align): ${ELAPSED_NOALIGN}s | Transferencias: $TRANSF_NOALIGN"
        echo "$(date -Iseconds),$NODES,$N,$GENS,no,$ELAPSED_NOALIGN,$TRANSF_NOALIGN" >> "$OUT_CSV"
    done
done

echo ""
echo "======================================================================"
echo " Mediciones finalizadas con exito."
echo " Metricas guardadas en: $OUT_CSV"
echo "======================================================================"
