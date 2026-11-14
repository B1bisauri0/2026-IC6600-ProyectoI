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
| Prototipo 2 | 20 nov | `v0.3-prototipo2` | Pendiente |
| Final | 27 nov | `v1.0-final` | Pendiente |

**Prototipo 1:** memoria (paginación, MSI, directorio, reemplazo FIFO/CLOCK),
red (conexiones TCP, membresía, heartbeat), locks, barreras, consultas y
Juego de la Vida en 1 a 3 nodos.

## Estructura del repositorio

```
.
├── README.md
├── Makefile                  # compila dsm_node, dsm_query y dsm_lab
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
│   ├── stats.c/.h            # estadísticas
│   ├── wire.h                # serialización de los cuerpos de mensaje
│   ├── network.c             # sockets, conexiones, lectores y pool de servicio
│   ├── puente.c              # une la parte de memoria con la red
│   ├── membership.c          # unión de nodos, tabla de nodos, heartbeat
│   ├── sync.c                # barreras y locks distribuidos
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
│   ├── dsm_query.c           # consulta el estado de los nodos
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
    └── entregable1/          # documento de diseño (PDF)
```

**Pendiente para el Prototipo 2:** `task.c` y `dsm_run` (ejecución distribuida
de aplicaciones), `replica.c` (réplicas y puntos de control), `apps/filestore/`
(almacén de archivos grandes) y consultas de páginas y estadísticas.

## Compilación

```bash
make            # compila dsm_node, dsm_query y tools/dsm_lab
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
./dsm_node --conf dsm_local.conf --id 0 --demo-lock 2 --demo-barrier 2
./dsm_node --conf dsm_local.conf --id 1 --demo-lock 2 --demo-barrier 2
./dsm_node --conf dsm_local.conf --id 2 --demo-lock 2 --demo-barrier 2
```

Consultar el estado del clúster desde otra terminal:

```bash
./dsm_query --port 27702 nodes
```

En el laboratorio, con las IP reales en `dsm.conf` (idéntico en las tres
computadoras), el id se detecta solo:

```bash
./dsm_node --conf dsm.conf
```

## Pruebas

```bash
bash tests/run_local.sh
```

Incluye coherencia MSI con 2 y 3 nodos, asignaciones grandes, reemplazo FIFO y
CLOCK, nodo caído y el Juego de la Vida comparado contra la versión secuencial.

## Notas

- Puertos del proyecto: **27700–27799**. Cada nodo usa `puerto_base`, `+1` y `+2`.
- `worker_threads=1` en los `.conf`: la parte de memoria necesita que los
  mensajes de un mismo nodo se atiendan en orden. Para el Prototipo 2 se
  cambiará a una cola por nodo de origen, que permite usar más hilos.
