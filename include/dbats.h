/*
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include <inttypes.h>
#include <db.h>
#include "dbats_log.h"

/* ************************************************** */

// Flags
#define DBATS_CREATE         0x01 // create database if it doesn't exist
#define DBATS_PRELOAD        0x04 // load fragments when tslice is selected
#define DBATS_READONLY       0x08 // don't allow writing
#define DBATS_UNCOMPRESSED   0x10 // don't compress data written to db

// Aggregation functions
#define DBATS_AGG_NONE   0
#define DBATS_AGG_MIN    1
#define DBATS_AGG_MAX    2
#define DBATS_AGG_AVG    3
#define DBATS_AGG_LAST   4
#define DBATS_AGG_SUM    5

// Aggregation parameters
typedef struct {
    int func;               // aggregation function
    int steps;              // number of primary data points in agg
    uint32_t period;        // length of slice (seconds)
    uint32_t first_flush;   // time of earliest flush
    uint32_t last_flush;    // time of latest flush
} dbats_agg;

#if 0
typedef uint32_t dbats_value;
#define PRIval PRIu32
#define SCNval SCNu32
#else
typedef uint64_t dbats_value;
#define PRIval PRIu64
#define SCNval SCNu64
#endif

typedef struct dbats_tslice dbats_tslice;
typedef struct dbats_key_info dbats_key_info_t;

typedef struct {
    uint32_t start;
    uint32_t end;
} dbats_timerange_t;

typedef struct {
    uint8_t  is_open;
    uint8_t  readonly;                   // Mode used to open the db file
    uint8_t  compress;                   // Compress fragments?
    uint16_t num_aggs;                   // Number of aggregations
    uint16_t values_per_entry;           // Number of dbats_values in an entry
    uint16_t entry_size;                 // Size of an entry (bytes)
    uint32_t num_keys;                   // Next available key_id
    uint32_t period;                     // length of raw time slice (sec)
    void *state_compress;
    void *state_decompress;
    DB_ENV *dbenv;                        // DB environment
    DB *dbMeta;                           // config parameters
    DB *dbKeys;                           // key name -> key_info
    DB *dbData;                           // {time, agg_id, frag_id} -> fragment
    DBC *keyname_cursor;                  // for iterating over key names
    uint32_t keyid_walker;                // for iterating over key ids
    dbats_tslice **tslice;                // a tslice for each aggregation level
    dbats_agg *agg;                       // parameters for each aggregation level
    dbats_key_info_t **key_info_block;
    uint8_t *db_get_buf;
    size_t db_get_buf_len;
} dbats_handler;

/* ************************************************** */

extern int dbats_open(dbats_handler *handler, const char *dbats_path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags);

extern int dbats_aggregate(dbats_handler *handler, int func, int steps);

extern void dbats_close(dbats_handler *handler);

extern uint32_t dbats_normalize_time(const dbats_handler *handler, int agg_id,
    uint32_t *time);

extern int dbats_goto_time(dbats_handler *handler,
    uint32_t time_value, uint32_t flags);

extern int dbats_get_key_id(dbats_handler *handler, const char *key,
    uint32_t *key_id, uint32_t flags);
extern const char *dbats_get_key_name(dbats_handler *handler, uint32_t key_id);

extern int dbats_set(dbats_handler *handler, uint32_t key_id,
    const dbats_value *valuep);
extern int dbats_set_by_key (dbats_handler *handler, const char *key,
    const dbats_value *valuep);

extern int dbats_get(dbats_handler *handler, uint32_t key_id,
    const dbats_value **valuepp, int agg_id);
extern int dbats_get_double(dbats_handler *handler, uint32_t key_id,
    const double **valuepp, int agg_id);
extern int dbats_get_by_key(dbats_handler *handler, const char *key,
    const dbats_value **valuepp, int agg_id);

extern int dbats_walk_keyname_start(dbats_handler *handler);
extern int dbats_walk_keyname_next(dbats_handler *handler, uint32_t *key_id_p);
extern int dbats_walk_keyname_end(dbats_handler *handler);

extern int dbats_walk_keyid_start(dbats_handler *handler);
extern int dbats_walk_keyid_next(dbats_handler *handler, uint32_t *key_id_p);
extern int dbats_walk_keyid_end(dbats_handler *handler);

extern void dbats_stat_print(const dbats_handler *handler);
