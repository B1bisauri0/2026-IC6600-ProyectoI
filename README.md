# Sistema DSM: Memoria Compartida Distribuida

Proyecto del curso **IC6600 Principios de Sistemas Operativos**, II Semestre 2026
Instituto Tecnológico de Costa Rica · Prof. M.Sc. Esteban Arias Méndez

**Integrantes**
- Alexandra Milagros De Gracia Calderón
- Tamara Villarevia Navarro

Sistema de memoria compartida distribuida en espacio de usuario sobre Linux, con
paginación bajo demanda (`mmap` + `SIGSEGV`), comunicación por sockets TCP y
coherencia mediante el protocolo MSI.

## Estado

| Entrega | Fecha | Etiqueta | Estado |
|---|---|---|---|
| Documento de diseño | 6 nov | `v0.1-diseno` | Entregado |
| Prototipo 1 | 13 nov | `v0.2-prototipo1` | Entregado |
| Prototipo 2 | 20 nov | `v0.3-prototipo2` | Entregado |
| Final | 27 nov | `v1.0-final` | Pendiente |

**Prototipo 1:** memoria (paginación, MSI, directorio, reemplazo FIFO/CLOCK),
red (conexiones TCP, membresía, heartbeat), locks, barreras, consultas y
Juego de la Vida en 1 a 3 nodos.

**Prototipo 2:** ejecución distribuida (`dsm_run --all`), consultas de
páginas, asignaciones y estadísticas de todo el sistema (`dsm_query pages`,
`allocs`, `stats`), cola por nodo de origen en la red y réplicas con puntos
de control.

## Estructura del repositorio

```
.
├── README.md
├── Makefile                  # compila dsm_node, dsm_query, dsm_run, dsm_lab y life.so
├── dsm.conf                  # configuración para las 3 computadoras del laboratorio
├── dsm_local.conf            # configuración para pruebas en una sola máquina
├── .gitignore
│
├── src/                      # proceso dsm_node
│   ├── main.c                # argumentos, arranque y apagado del nodo
│   ├── config.c              # lectura y validación de dsm.conf
│   ├── dsm.c                 # API pública, asignación de memoria, despacho de mensajes
│   ├── dsm_internal.h        # estado interno compartido de la parte de memoria
│   ├── pager.c/.h            # manejador de SIGSEGV, hilo Pager, transiciones MSI
│   ├── directory.c/.h        # directorio de páginas del nodo home
│   ├── replica.c/.h          # réplicas, puntos de control y recuperación
│   ├── stats.c/.h            # estadísticas
│   ├── wire.h                # serialización de los cuerpos de mensaje
│   ├── network.c             # sockets, conexiones, lectores y pool de servicio
│   ├── puente.c              # une la parte de memoria con la red
│   ├── espera.c/.h           # preguntar algo a otro nodo y esperar la respuesta
│   ├── membership.c          # unión de nodos, tabla de nodos, heartbeat
│   ├── sync.c                # barreras y locks distribuidos
│   ├── task.c/.h             # carga de aplicaciones (dlopen) y ejecución distribuida
│   └── query.c               # servidor de consultas (puerto_base + 2)
│
├── include/
│   ├── dsm.h                 # encabezado que usan las aplicaciones
│   ├── dsm_types.h           # tipos y mensajes comunes (sección 5 y 7 del diseño)
│   ├── config.h              # estructura de configuración
│   ├── protocol.h            # interfaz de envío de mensajes
│   ├── network.h
│   ├── membership.h
│   ├── sync.h
│   └── query.h
│
├── tools/
│   ├── dsm_run.c             # lanza una aplicación en el nodo local o en todos
│   ├── dsm_query.c           # consulta nodos, páginas, asignaciones y estadísticas
│   └── dsm_lab.c             # verificación de la computadora antes de usar el DSM
│
├── apps/
│   └── life/                 # Juego de la Vida distribuido
│       ├── life.c            # versión DSM
│       ├── life_ref.c        # versión secuencial de referencia (para comparar)
│       └── patterns/         # patrones RLE (glider, R-pentomino, Gosper gun)
│
├── tests/                    # pruebas de la parte de memoria y del Juego de la Vida
│   └── run_local.sh          # corre todas las pruebas en una sola máquina
│
└── docs/
    ├── entregable1/          # documento de diseño (PDF)
    └── entregable2/          # documento del Prototipo 1 (PDF)
```

**Pendiente para el Final:** `apps/filestore/` (almacén de archivos grandes),
heartbeat por su propio puerto (`puerto_base + 1`), pruebas en el laboratorio,
informe y manual de usuario.

## Compilación

```bash
make            # compila dsm_node, dsm_query, dsm_run, tools/dsm_lab y apps/life/life.so
make clean      # borra lo compilado
make asan       # compila con AddressSanitizer
```

## Uso

Verificar la computadora:

```bash
./tools/dsm_lab
```

Tres nodos en una sola máquina, cada uno en su terminal:

```bash
./dsm_node --conf dsm_local.conf --id 0
./dsm_node --conf dsm_local.conf --id 1
./dsm_node --conf dsm_local.conf --id 2
```

Para probar solo locks y barreras, sin aplicación, se puede agregar
`--demo-lock 2 --demo-barrier 2` a cada nodo.

En el laboratorio, con las IP reales en `dsm.conf` (idéntico en las tres
computadoras), el id se detecta solo:

```bash
./dsm_node --conf dsm.conf
```

### Correr una aplicación

Con los nodos ya arrancados. `dsm_node` tiene que correr desde la carpeta del
repositorio en cada computadora, para que todos encuentren la `.so` y los
patrones:

```bash
./dsm_run apps/life/life.so 512 50 --threads 2            # solo en este nodo
./dsm_run --all apps/life/life.so 2000 100 --threads 4    # en todos los nodos
./dsm_run --all apps/life/life.so 300 50 --rle apps/life/patterns/gosper_gun.rle
```

Lo que la aplicación imprime con `dsm_printf` sale en la terminal de `dsm_run`.
Para comprobar el resultado, el `grid_hash` debe ser igual al de la versión
secuencial:

```bash
tests/life_ref 300 50 --rle apps/life/patterns/gosper_gun.rle | tail -1
```

### Consultas

Desde la misma computadora que el nodo:

```bash
./dsm_query nodes        # estado y memoria de cada nodo
./dsm_query pages        # tabla de páginas de todo el sistema
./dsm_query pages 1      # solo las páginas cuyo home es el nodo 1
./dsm_query allocs       # asignaciones y orden de sus páginas
./dsm_query stats        # estadísticas por nodo y total
```

En pruebas locales, cada nodo tiene su puerto de consultas (puerto base + 2):
`--port 27002`, `--port 27012` o `--port 27022`.

## Pruebas

```bash
bash tests/run_local.sh
```

Incluye coherencia MSI con 2 y 3 nodos, asignaciones grandes, reemplazo FIFO y
CLOCK, nodo caído, réplicas con recuperación y el Juego de la Vida comparado
contra la versión secuencial.

## Notas

- Puertos del proyecto: **27000–27099**. Cada nodo usa `puerto_base`, `+1` y `+2`.
- La red tiene una cola por nodo de origen: los mensajes de un mismo nodo se
  atienden en orden y de uno en uno (lo que necesita la parte de memoria), y
  los de nodos distintos en paralelo. Por eso `worker_threads` puede ser mayor que 1.