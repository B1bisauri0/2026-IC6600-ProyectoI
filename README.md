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
| Prototipo 1 | 13 nov | `v0.2-prototipo1` | En desarrollo |
| Prototipo 2 | 20 nov | `v0.3-prototipo2` | Pendiente |
| Final | 27 nov | `v1.0-final` | Pendiente |
 
## Estructura del repositorio
 
```
.
├── README.md
├── Makefile                  # compila dsm_node, herramientas y aplicaciones
├── dsm.conf                  # configuración para las 3 computadoras del laboratorio
├── dsm_local.conf            # configuración para pruebas en una sola máquina
├── .gitignore
│
├── src/                      # proceso dsm_node
│   ├── main.c                # argumentos, arranque y apagado del nodo
│   ├── config.c/.h           # lectura y validación de dsm.conf
│   ├── dsm.c/.h              # API pública, asignación de memoria, registro de hilos
│   ├── pager.c/.h            # manejador de SIGSEGV, hilo Pager, transiciones MSI
│   ├── directory.c/.h        # directorio de páginas del nodo home
│   ├── network.c/.h          # sockets, listener, serialización de mensajes
│   ├── membership.c/.h       # unión de nodos, tabla de nodos, heartbeat
│   ├── sync.c/.h             # barreras y locks distribuidos
│   ├── task.c/.h             # carga de aplicaciones (dlopen), ejecución distribuida
│   ├── replica.c/.h          # réplicas, puntos de control y recuperación
│   ├── stats.c/.h            # estadísticas
│   └── query.c/.h            # consultas (CLI y exportación HTML)
│
├── include/
│   └── dsm.h                 # encabezado que usan las aplicaciones
│
├── tools/
│   ├── dsm_run.c             # lanza una aplicación en el nodo local o en todos
│   ├── dsm_query.c           # consulta nodos, páginas, asignaciones y estadísticas
│   └── dsm_lab.c             # verificación de la computadora antes de usar el DSM
│
├── apps/
│   ├── life/                 # Juego de la Vida distribuido (base: pthreadslife, MIT)
│   │   └── patterns/         # patrones RLE de LifeWiki
│   └── filestore/            # almacén de archivos grandes (SHA-256 de B. Conte)
│
├── tests/                    # pruebas T1 a T13 del plan de pruebas
│
└── docs/
    ├── entregable1/          # documento de diseño (LaTeX y PDF)
    ├── prototipo1/
    ├── prototipo2/
    └── final/                # informe y manual de usuario
```

## Compilación y uso (planeado)
 
```bash
make                                   # compila todo
./tools/dsm_lab                        # verificar la computadora (prueba T1)
 
./dsm_node --conf dsm.conf             # terminal 1: el nodo queda corriendo
./dsm_run apps/life.so 4000 100        # terminal 2: lanzar una aplicación
./dsm_query nodes                      # consultar el estado del clúster
```
 
Pruebas en una sola máquina (tres nodos con puertos distintos):
 
```bash
./dsm_node --conf dsm_local.conf --id 0
./dsm_node --conf dsm_local.conf --id 1
./dsm_node --conf dsm_local.conf --id 2
```
 
Puertos del proyecto: **27700–27799**. Cada nodo usa `puerto_base`, `+1` y `+2`.
