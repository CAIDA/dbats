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

#define DBATS_DB_VERSION     1

#define DBATS_KEYLEN         128 // max length of key name

// Flags
#define DBATS_CREATE         0x01 // create database if it doesn't exist
#define DBATS_PRELOAD        0x04 // load fragments when tslice is selected
#define DBATS_READONLY       0x08 // don't allow writing
#define DBATS_UNCOMPRESSED   0x10 // don't compress data written to db
#define DBATS_EXCLUSIVE      0x20 // obtain exclusive lock on whole db
#define DBATS_NO_TXN         0x40 // don't use transactions (for debugging only)

// Aggregation functions
#define DBATS_AGG_NONE   0
#define DBATS_AGG_MIN    1
#define DBATS_AGG_MAX    2
#define DBATS_AGG_AVG    3
#define DBATS_AGG_LAST   4
#define DBATS_AGG_SUM    5

extern const char *dbats_agg_func_label[];

typedef struct {
    uint32_t start;
    uint32_t end;
} dbats_timerange_t;

// Aggregation parameters
typedef struct {
    uint32_t func;           // aggregation function
    uint32_t steps;          // # of data points contributing to one agg value
    uint32_t period;         // time covered by one full agg value (seconds)
    dbats_timerange_t times; // times of first and last data point
} dbats_agg;

typedef struct {
    uint32_t version;          // db version
    uint8_t readonly;          // Mode used to open the db
    uint8_t compress;          // Compress data in db?
    uint8_t exclusive;         // Obtain exclusive lock on whole db
    uint8_t no_txn;            // Don't use transactions (unsafe)
    uint16_t num_aggs;         // Number of aggregations
    uint16_t values_per_entry; // Number of dbats_values in an entry
    uint16_t entry_size;       // Size of an entry (bytes)
    uint32_t num_keys;         // Number of keys
    uint32_t period;           // length of raw time slice (seconds)
} dbats_config;

typedef uint64_t dbats_value;
#define PRIval PRIu64
#define SCNval SCNu64

typedef struct dbats_handler dbats_handler;

/* ************************************************** */

extern dbats_handler *dbats_open(const char *dbats_path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags);

extern int dbats_aggregate(dbats_handler *handler, int func, int steps);

extern void dbats_close(dbats_handler *handler);

extern uint32_t dbats_normalize_time(const dbats_handler *handler, int agg_id,
    uint32_t *time);

extern int dbats_select_time(dbats_handler *handler,
    uint32_t time_value, uint32_t flags);

extern int dbats_get_key_id(dbats_handler *handler, const char *key,
    uint32_t *key_id, uint32_t flags);
extern int dbats_get_key_name(dbats_handler *handler, uint32_t key_id,
    char *namebuf);

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
extern int dbats_walk_keyname_next(dbats_handler *handler, uint32_t *key_id_p,
    char *namebuf);
extern int dbats_walk_keyname_end(dbats_handler *handler);

extern int dbats_walk_keyid_start(dbats_handler *handler);
extern int dbats_walk_keyid_next(dbats_handler *handler, uint32_t *key_id_p,
    char *namebuf);
extern int dbats_walk_keyid_end(dbats_handler *handler);

extern const volatile dbats_config *dbats_get_config(dbats_handler *handler);
extern const volatile dbats_agg *dbats_get_agg(dbats_handler *handler, int agg_id);

extern void dbats_stat_print(const dbats_handler *handler);
