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
/** @file dbats.h
    @brief DBATS API header file
*/

#include <inttypes.h>
#include <db.h>
#include "dbats_log.h"

/* ************************************************** */

#define DBATS_DB_VERSION     1   ///< Version of db format written by this API

#define DBATS_KEYLEN         128 ///< max length of key name

// Flags
#define DBATS_CREATE         0x01 ///< create object if it doesn't exist
#define DBATS_PRELOAD        0x04 ///< load fragments when tslice is selected
#define DBATS_READONLY       0x08 ///< don't allow writing
#define DBATS_UNCOMPRESSED   0x10 ///< don't compress written timeseries data
#define DBATS_EXCLUSIVE      0x20 ///< obtain exclusive lock on whole db
#define DBATS_NO_TXN         0x40 ///< don't use transactions (for debugging only)

// Aggregation functions
#define DBATS_AGG_NONE   0 ///< primary data series, not an aggregate
#define DBATS_AGG_MIN    1 ///< minimum
#define DBATS_AGG_MAX    2 ///< maximum
#define DBATS_AGG_AVG    3 ///< average
#define DBATS_AGG_LAST   4 ///< last
#define DBATS_AGG_SUM    5 ///< sum

/// Labels for aggregation functions
extern const char *dbats_agg_func_label[];

typedef struct {
    uint32_t start;
    uint32_t end;
} dbats_timerange_t;

/// Aggregation parameters (read only).
typedef struct {
    uint32_t func;           ///< aggregation function
    uint32_t steps;          ///< # of data points contributing to one agg value
    uint32_t period;         ///< time covered by one full agg value (seconds)
    dbats_timerange_t times; ///< times of first and last data points
} dbats_agg;

/** Configuration parameters (read only).
 *  Fields marked "(C)" are fixed when the database is created;
 *  fields marked "(O)" are fixed when the database is opened;
 *  fields marked "(V)" are variable, i.e. may change while the db is open.
 */
typedef struct {
    uint32_t version;          ///< Version of db format (C)
    uint8_t readonly;          ///< Disallow writing to db (O)
    uint8_t compress;          ///< Compress data written to db (O)
    uint8_t exclusive;         ///< Obtain exclusive lock on whole db (O)
    uint8_t no_txn;            ///< Don't use transactions (O)
    uint16_t num_aggs;         ///< Number of aggregations (V)
    uint16_t values_per_entry; ///< Number of dbats_values in an entry (C)
    uint16_t entry_size;       ///< Size of an entry in bytes (C)
    uint32_t num_keys;         ///< Number of keys (V)
    uint32_t period;           ///< Primary sample period in seconds (C)
} dbats_config;

typedef uint64_t dbats_value;  ///< A value stored in a DBATS database.
#define PRIval PRIu64          ///< printf() conversion specifier for dbats_value
#define SCNval SCNu64          ///< scanf() conversion specifier for dbats_value

typedef struct dbats_handler dbats_handler; ///< opaque DBATS handle

/* ************************************************** */

/** Open a DBATS database.
 *  @param[in] dbats_path path of a directory containing a DBATS database
 *  @param[in] values_per_entry number of dbats_values in the array read by
 *    dbats_get() or written by dbats_set().
 *  @param[in] period the number of seconds between data values
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    - TSDB_CREATE - create the database if it doesn't already exist
 *    - TSDB_READONLY - do not allow writing to the database
 *    - TSDB_UNCOMPRESSED - do not compress written timeseries data
 *    - TSDB_EXCLUSIVE - do not allow any other process to access the database
 *    - TSDB_NO_TXN - do not use transactions (unsafe)
 *  @return On success, returns a pointer to an opaque dbats_handler that
 *  must be passed to all subsequent calls on this database.  On failure,
 *  returns NULL.
 */
extern dbats_handler *dbats_open(const char *dbats_path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags);

/** Defines an aggregate.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] func one of the following values indicating which aggregation
 *    function to apply to the data points:
 *    - DBATS_AGG_MIN - minimum
 *    - DBATS_AGG_MAX - maximum
 *    - DBATS_AGG_AVG - average
 *    - DBATS_AGG_LAST - last
 *    - DBATS_AGG_SUM - sum
 *  @param[in] steps number of data points to included in an aggregate
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_aggregate(dbats_handler *handler, int func, int steps);

/** Close a database opened by dbats_open().
 *  @param[in] handler A dbats_handler created by dbats_open().
 */
extern void dbats_close(dbats_handler *handler);

/** Round a time value down to a multiple of period.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] agg_id which aggregate's period to use (use 0 for the primary
 *    data series).
 *  @param[in] time a unix time value (seconds since 1970-01-01 00:00:00 UTC,
 *    without leap seconds).  Value will be rounded down to the nearest
 *    multiple of the data period, counting from 00:00 on the first Sunday of
 *    1970.
 *  @return the rounded time value
 */
extern uint32_t dbats_normalize_time(const dbats_handler *handler, int agg_id,
    uint32_t *time);

/** Select a time period to operate on in subsequent calls to dbats_get() and
 *  dbats_set().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] time_value the desired time, which will be rounded down as
 *    by dbats_normalize_time(handler, 0, &time_value).
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    - DBATS_PRELOAD - load data immediately instead of waiting until it's
 *      needed
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_select_time(dbats_handler *handler,
    uint32_t time_value, uint32_t flags);

/** Get the id of a key.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key the name of the key.
 *  @param[out] key_id_p a pointer to a uint32_t where the id for key will be
 *    written.
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    - DBATS_CREATE - create the key if it does not already exist.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get_key_id(dbats_handler *handler, const char *key,
    uint32_t *key_id_p, uint32_t flags);

/** Get the name of a key.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key_id the id of the key.
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get_key_name(dbats_handler *handler, uint32_t key_id,
    char *namebuf);

/** Write a value to the database for the specified key and the time selected
 *  by dbats_select_time().
 *  Note that dbats_set() is much faster than dbats_set_by_key() if you know
 *  the key_id already.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key_id the id of the key.
 *  @param[in] valuep a pointer to an array of values_per_entry dbats_value
 *    to be written to the database.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_set(dbats_handler *handler, uint32_t key_id,
    const dbats_value *valuep);

/** Write a value to the database for the specified key and the time selected
 *  by dbats_select_time().
 *  Equivalent to dbats_get_key_id() followed by dbats_set().
 *  Note that dbats_set() is much faster than dbats_set_by_key() if you know
 *  the key_id already.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key the name of the key.
 *  @param[in] valuep a pointer to an array of values_per_entry dbats_values
 *    to be written to the database.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_set_by_key (dbats_handler *handler, const char *key,
    const dbats_value *valuep);

/** Read a uint32_t value from the database for the specified key and the
 *  time selected by dbats_select_time().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key_id the id of the key.
 *  @param[out] valuepp the address of a dbats_value*; after the call,
 *    *valuepp will point to an array of values_per_entry dbats_values
 *    read from the database.
 *  @param[in] agg_id which aggregate's value to get (use 0 for the primary
 *    data series).
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get(dbats_handler *handler, uint32_t key_id,
    const dbats_value **valuepp, int agg_id);

/** Read a double value from the database for the specified key and the
 *  time selected by dbats_select_time().
 *  This function can be applied only to DBATS_AGG_AVG values.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key_id the id of the key.
 *  @param[out] valuepp the address of a dbats_value*; after the call,
 *    *valuepp will point to an array of values_per_entry double values
 *    read from the database.
 *  @param[in] agg_id which aggregate's value to get (use 0 for the primary
 *    data series).
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get_double(dbats_handler *handler, uint32_t key_id,
    const double **valuepp, int agg_id);

/** Read a uint32_t value from the database for the specified key and the
 *  time selected by dbats_select_time().
 *  Equivalent to dbats_get_key_id() followed by dbats_get().
 *  Note that dbats_get() is significantly faster if you already
 *  know the key_id.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key the name of the key.
 *  @param[out] valuepp the address of a dbats_value*; after the call,
 *    *valuepp will point to an array of values_per_entry dbats_values
 *    read from the database.
 *  @param[in] agg_id which aggregate's value to get (use 0 for the primary
 *    data series).
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get_by_key(dbats_handler *handler, const char *key,
    const dbats_value **valuepp, int agg_id);

/** Prepare to iterate over the list of keys ordered by name.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */

extern int dbats_walk_keyname_start(dbats_handler *handler);
/** Get the next key id and key name in the sequence started by
 *  dbats_walk_keyname_start().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[out] key_id_p A pointer to a uint32_t where the key_id will be written.
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyname_next(dbats_handler *handler, uint32_t *key_id_p,
    char *namebuf);

/** End the sequence started by dbats_walk_keyname_start().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyname_end(dbats_handler *handler);

/** Prepare to iterate over the list of keys ordered by id.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyid_start(dbats_handler *handler);

/** Get the next key id and key name in the sequence started by
 *  dbats_walk_keyid_start().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[out] key_id_p A pointer to a uint32_t where the key_id will be written.
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyid_next(dbats_handler *handler, uint32_t *key_id_p,
    char *namebuf);

/** End the sequence started by dbats_walk_keyid_start().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyid_end(dbats_handler *handler);

/** Get configuration parameters from a DBATS database.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return pointer to a dbats_config containing configuration parameters.
 */
extern const volatile dbats_config *dbats_get_config(dbats_handler *handler);

/** Get aggregation parameters from a DBATS database.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] agg_id The id of the aggregate.
 *  @return pointer to a dbats_agg describing the aggregate specified by agg_id.
 */
extern const volatile dbats_agg *dbats_get_agg(dbats_handler *handler, int agg_id);

extern void dbats_stat_print(const dbats_handler *handler);
