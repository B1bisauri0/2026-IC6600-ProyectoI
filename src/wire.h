/* wire.h -- serializacion en orden de red de los cuerpos de mensaje (sec. 8.4). */
#ifndef DSM_WIRE_H
#define DSM_WIRE_H
#include <arpa/inet.h>
#include <stdint.h>
#include <string.h>
#include <endian.h>

static inline void w16(uint8_t **p, uint16_t v) { v = htons(v); memcpy(*p, &v, 2); *p += 2; }
static inline void w32(uint8_t **p, uint32_t v) { v = htonl(v); memcpy(*p, &v, 4); *p += 4; }
static inline void w64(uint8_t **p, uint64_t v) { v = htobe64(v); memcpy(*p, &v, 8); *p += 8; }
static inline void w8 (uint8_t **p, uint8_t v)  { **p = v; *p += 1; }
static inline uint16_t r16(const uint8_t **p) { uint16_t v; memcpy(&v, *p, 2); *p += 2; return ntohs(v); }
static inline uint32_t r32(const uint8_t **p) { uint32_t v; memcpy(&v, *p, 4); *p += 4; return ntohl(v); }
static inline uint64_t r64(const uint8_t **p) { uint64_t v; memcpy(&v, *p, 8); *p += 8; return be64toh(v); }
static inline uint8_t  r8 (const uint8_t **p) { uint8_t v = **p; *p += 1; return v; }
#endif
