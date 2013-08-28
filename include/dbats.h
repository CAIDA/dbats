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
 *  DBATS API header file.
 *
 *  DBATS is a database for storing millions of similar time series and
 *  aggregating them by time.
 *  A DBATS time series is a sequence of data entries measured at regular time
 *  intervals.
 *  A set of similar time series with the same period and entry size is a
 *  "bundle".  Each time series in a bundle is identified by a user-defined
 *  string key and an automatically assigned integer key id.
 *  The primary bundle stores the original raw data.
 *  Additional "aggregate" bundles can be defined that merge sub-sequences
 *  of data points for a key in the primary bundle into single data points
 *  for the same key in the aggregate bundle.
 *
 *  Typical usage:
 *  - Open a database with dbats_open().
 *  - If this is a new database, define aggregate time series with
 *    dbats_aggregate().
 *  - Optionally, dbats_commit().
 *  - Loop:
 *    - Select a working time slice with dbats_select_time().
 *    - Write primary values for multiple keys with dbats_set(), and/or
 *    - read primary and/or aggregate values for multiple keys with dbats_get().
 *    - Optionally, dbats_commit().
 *  - Close the database with dbats_close().
 *
 *  Functions that return an int will return one of the following types of
 *  values:
 *    - 0 for success
 *    - a (positive) E* constant defined in <errno.h> for a system error
 *    - a (negative) DB_* constant defined in <db.h> for a DB error
 *    - -1 for other error
*/

#include <inttypes.h>
#include <db.h>
#include "dbats_log.h"

/* ************************************************** */

#define DBATS_DB_VERSION     4   ///< Version of db format written by this API

#define DBATS_KEYLEN         128 ///< max length of key name

/** @name Flags */
///@{
#define DBATS_CREATE       0x0001 ///< create object if it doesn't exist
#define DBATS_PRELOAD      0x0004 ///< load fragments when tslice is selected
#define DBATS_READONLY     0x0008 ///< don't allow writing
#define DBATS_UNCOMPRESSED 0x0010 ///< don't compress written time series data
#define DBATS_EXCLUSIVE    0x0020 ///< obtain exclusive lock on whole db
#define DBATS_NO_TXN       0x0040 ///< don't use transactions (for debugging only)
#define DBATS_UPDATABLE    0x0080 ///< allow updates to existing values
#define DBATS_MULTIWRITE   0x0100 ///< allow multiple processes to write in parallel (experimental - do not use)
///@}

/** @name Aggregation functions */
///@{
#define DBATS_AGG_NONE   0 ///< primary bundle, not an aggregate
#define DBATS_AGG_MIN    1 ///< minimum
#define DBATS_AGG_MAX    2 ///< maximum
#define DBATS_AGG_AVG    3 ///< average. Values can be fetched as dbats_value or double.
#define DBATS_AGG_LAST   4 ///< last
#define DBATS_AGG_SUM    5 ///< sum
///@}

/// Labels for aggregation functions
extern const char *dbats_agg_func_label[];

/// Time series bundle parameters (read only).
typedef struct {
    uint32_t func;           ///< aggregation function
    uint32_t steps;          ///< # of data points contributing to one agg value
    uint32_t period;         ///< time covered by one value (seconds)
    uint32_t keep;           ///< number of data points to keep (0 means keep all)
} dbats_bundle_info;

/** Configuration parameters (read only).
 *  Fields marked "(C)" are fixed when the database is created;
 *  fields marked "(O)" are fixed when the database is opened.
 */
typedef struct {
    uint32_t version;          ///< Version of db format (C)
    uint8_t readonly;          ///< Disallow writing to db (O)
    uint8_t compress;          ///< Compress data written to db (O)
    uint8_t exclusive;         ///< Obtain exclusive lock on whole db (O)
    uint8_t no_txn;            ///< Don't use transactions (O)
    uint8_t updatable;         ///< Allow updates to existing values (O)
    uint16_t num_bundles;      ///< Number of time series bundles (C)
    uint16_t values_per_entry; ///< Number of dbats_values in an entry (C)
    uint16_t entry_size;       ///< Size of an entry in bytes (C)
    uint32_t period;           ///< Primary sample period in seconds (C)
} dbats_config;

typedef uint64_t dbats_value;  ///< A value stored in a DBATS database.
#define PRIval PRIu64          ///< printf() conversion specifier for dbats_value
#define SCNval SCNu64          ///< scanf() conversion specifier for dbats_value

typedef struct dbats_handler dbats_handler; ///< Opaque handle for a DBATS db

/* ************************************************** */

/** Open an existing or new DBATS database.
 *  Some configuration parameters can be set only when creating a database;
 *  when opening an existing database, those parameters will be silently
 *  ignored.
 *  By default, the database will allow reading data values and inserting new
 *  data values, but not updating existing data values.
 *  This function also begins a transaction, so that database creation and
 *  subsequent dbats_aggregate() calls will not have race conditions with
 *  other processes.  If there will be a delay before your first call to
 *  dbats_select_time(), you should call dbats_commit() to release the locks
 *  and allow other processes to access the database during the delay.
 *  @param[in] dbats_path path of a directory containing a DBATS database
 *  @param[in] values_per_entry number of dbats_values in the array read by
 *    dbats_get() or written by dbats_set()
 *    (only if creating a new database).
 *  @param[in] period the number of seconds between data values
 *    (only if creating a new database).
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    -	DBATS_CREATE - create the database if it doesn't already exist
 *    -	DBATS_READONLY - do not allow writing to the database (improves
 *      performance).
 *    -	DBATS_UNCOMPRESSED - do not compress written time series data
 *    -	DBATS_EXCLUSIVE - do not allow any other process to access the database
 *    -	DBATS_NO_TXN - do not use transactions (unsafe)
 *    -	DBATS_UPDATABLE - allow updates to existing values
 *  @return On success, returns a pointer to an opaque dbats_handler that
 *  must be passed to all subsequent calls on this database.  On failure,
 *  returns NULL.
 */
extern dbats_handler *dbats_open(const char *dbats_path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags);

/** Close a database opened by dbats_open().
 *  Automatically calls dbats_commit() if needed to commit any operations since
 *  the last call to dbats_open() or dbats_select_time().
 *  @param[in] handler A dbats_handler created by dbats_open().
 */
extern void dbats_close(dbats_handler *handler);

/** Defines an aggregate.
 *  Should be called after opening a database with dbats_open() but before
 *  dbats_commit() or dbats_select_time(), and before any data has been
 *  written to the database.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] func one of the following values indicating which aggregation
 *    function to apply to the data points:
 *    - DBATS_AGG_MIN - minimum
 *    - DBATS_AGG_MAX - maximum
 *    - DBATS_AGG_AVG - average
 *    - DBATS_AGG_LAST - last
 *    - DBATS_AGG_SUM - sum
 *  @param[in] steps number of primary data points to include in an aggregate
 *    point
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_aggregate(dbats_handler *handler, int func, int steps);

/** Get the earliest timestamp in a time series bundle.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @param[out] start A pointer to a uint32_t that will be filled in with
 *    the time of the earliest entry in the bundle.
 *  @return 0 for success, DB_NOTFOUND if the bundle is empty,
 *    or other nonzero value for error.
 */
extern int dbats_get_start_time(dbats_handler *handler, int bid, uint32_t *start);

/** Get the latest timestamp in a time series bundle.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @param[out] end A pointer to a uint32_t that will be filled in with
 *    the time of the latest entry in the bundle.
 *  @return 0 for success, DB_NOTFOUND if the bundle is empty,
 *    or other nonzero value for error.
 */
extern int dbats_get_end_time(dbats_handler *handler, int bid, uint32_t *end);

/** Congfiure the number of data points to keep for each time series of a
 *  bundle.
 *  When a transaction started by dbats_select_time() is committed, any data
 *  points that are <code>keep</code> periods or more older than the latest
 *  point ever set (for any key) will be deleted.
 *  Once a point is deleted for being outside the limit, it can not be set
 *  again, even if the limit is changed.
 *  The limit for the primary data bundle (bid 0) can not be smaller than the
 *  number of steps in the smallest aggregate.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @param[in] keep The number of data points to keep in each time series of
 *    the bundle, or 0 to keep all points.
 *  @return
 *    - 0 for success;
 *    - EINVAL if bid does not refer to a valid bundle or if the limit is too
 *      small;
 *    - other nonzero value for other errors.
 */
extern int dbats_series_limit(dbats_handler *handler, int bid, int keep);

/** Round a time value down to a multiple of a bundle's period.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @param[in,out] t a unix time value (seconds since 1970-01-01 00:00:00
 *    UTC, without leap seconds).  Value will be rounded down to the nearest
 *    multiple of the data period, counting from 00:00 on the first Sunday of
 *    1970.
 *  @return the rounded time value (*t)
 */
extern uint32_t dbats_normalize_time(const dbats_handler *handler,
    int bid, uint32_t *t);

/** Select a time period to operate on in subsequent calls to dbats_get() and
 *  dbats_set().
 *  This function automatically calls dbats_commit() if needed to commit any
 *  operations since the last call to dbats_open() or dbats_select_time().
 *  Then this function selects a time period and begins a new transaction.
 *  All subsequent calls to dbats_get() and dbats_set() will take place in this
 *  transaction, up to the next call to dbats_select_time(), dbats_commit(),
 *  dbats_abort(), or dbats_close().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] time_value the desired time, which will be rounded down by
 *    dbats_normalize_time().
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    - DBATS_PRELOAD - load data immediately instead of waiting until it's
 *      needed (rarely useful)
 *  @return
 *    - 0 for success;
 *    - EINVAL if the database was not opened with DBATS_READONLY and the
 *      selected time is outside the keep limit set by dbats_series_limit();
 *    - other nonzero value for other errors.
 */
extern int dbats_select_time(dbats_handler *handler,
    uint32_t time_value, uint32_t flags);

/** Commit the transaction in progress, flushing any pending writes to the
 *  database and releasing any associated database locks and other resources.
 *  Calling this directly is useful if there will be a delay before your next
 *  call to dbats_select_time() and there are other processes trying to access
 *  the database.  After calling this function, you must call
 *  dbats_select_time() again before you can call dbats_set() or dbats_get().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_commit(dbats_handler *handler);

/** Abort the transaction in progress (since the last dbats_select_time()),
 *  discarding any pending writes and releasing any associated database locks
 *  and other resources.  After calling this function, you must call
 *  dbats_select_time() again before you can call dbats_set() or dbats_get().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_abort(dbats_handler *handler);

/** Get the id of a new or existing key.
 *  The key must already exist unless the DBATS_CREATE flag is given.
 *  When creating multiple keys, it is faster to do so inside a transaction.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key the name of the key.
 *  @param[out] key_id_p a pointer to a uint32_t where the id for key will be
 *    written.
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    - DBATS_CREATE - create the key if it does not already exist.
 *  @return 0 for success, DB_NOTFOUND if the key does not exist and
 *    DBATS_CREATE flag was not set, or other nonzero value for error.
 */
extern int dbats_get_key_id(dbats_handler *handler, const char *key,
    uint32_t *key_id_p, uint32_t flags);

/** Get the name of an existing key.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key_id the id of the key.
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get_key_name(dbats_handler *handler, uint32_t key_id,
    char *namebuf);

/** Write a value to the primary time series for the specified key and the
 *  time selected by dbats_select_time().
 *  All aggregate values whose time ranges contain this primary data point
 *  will also be updated.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key_id the id of the key.
 *  @param[in] valuep a pointer to an array of values_per_entry dbats_value
 *    to be written to the database.
 *  @return
 *    - 0 for success;
 *    - EPERM if the handler is not open or was opened with DBATS_READONLY;
 *    - EEXIST if a value already exists for the given time and key and handler
 *      was not opened with DBATS_UPDATABLE;
 *    - other nonzero value for other errors.
 */
extern int dbats_set(dbats_handler *handler, uint32_t key_id,
    const dbats_value *valuep);

/** Write a value to the primary time series for the specified key and the
 *  time selected by dbats_select_time().
 *  Equivalent to @ref dbats_get_key_id (handler, key, &key_id, flags)
 *  followed by @ref dbats_set (handler, key_id, valuep).
 *  Note that dbats_set() is much faster than dbats_set_by_key() if you know
 *  the key_id already.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key the name of the key.
 *  @param[in] valuep a pointer to an array of values_per_entry dbats_values
 *    to be written to the database.
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    - DBATS_CREATE - create the key if it does not already exist.
 *  @return
 *    - 0 for success;
 *    - EPERM if the handler is not open or was opened with DBATS_READONLY;
 *    - DB_NOTFOUND if the key does not exist and DBATS_CREATE flag was not set;
 *    - EEXIST if a value already exists for the given time and key, and
 *      handler was not opened with DBATS_UPDATABLE;
 *    - other nonzero value for other errors.
 */
extern int dbats_set_by_key (dbats_handler *handler, const char *key,
    const dbats_value *valuep, int flags);

/** Read an entry (array of dbats_value) from the database for the specified
 *  bundle id and key and the time selected by dbats_select_time().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key_id the id of the key.
 *  @param[out] valuepp the address of a dbats_value*; after the call,
 *    *valuepp will point to an array of values_per_entry dbats_values
 *    read from the database.
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get(dbats_handler *handler, uint32_t key_id,
    const dbats_value **valuepp, int bid);

/** Read an array of double values from the database for the specified bundle
 *  id and key and the time selected by dbats_select_time().
 *  This function can be applied only to DBATS_AGG_AVG values.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key_id the id of the key.
 *  @param[out] valuepp the address of a double*; after the call,
 *    *valuepp will point to an array of values_per_entry double values
 *    read from the database.
 *  @param[in] bid bundle id
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get_double(dbats_handler *handler, uint32_t key_id,
    const double **valuepp, int bid);

/** Read an array of dbats_values from the database for the specified bundle
 *  id and key and the time selected by dbats_select_time().
 *  Equivalent to dbats_get_key_id() followed by dbats_get().
 *  Note that dbats_get() is significantly faster if you already
 *  know the key_id.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] key the name of the key.
 *  @param[out] valuepp the address of a dbats_value*; after the call,
 *    *valuepp will point to an array of values_per_entry dbats_values
 *    read from the database.
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get_by_key(dbats_handler *handler, const char *key,
    const dbats_value **valuepp, int bid);

/** Get the number of keys in the database.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[out] num_keys A pointer to a uint32_t that will be filled in with
 *    the number of keys in the databasse.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_num_keys(dbats_handler *handler, uint32_t *num_keys);

/** Prepare to iterate over the list of keys ordered by name.
 *  You must call dbats_walk_keyname_end() before calling dbats_commit(),
 *  dbats_select_time(), or dbats_close().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyname_start(dbats_handler *handler);

/** Get the next key id and key name in the sequence started by
 *  dbats_walk_keyname_start().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[out] key_id_p A pointer to a uint32_t where the key_id will be written.
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written, or NULL if you do not
 *    need the name.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyname_next(dbats_handler *handler, uint32_t *key_id_p,
    char *namebuf);

/** End the sequence started by dbats_walk_keyname_start().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyname_end(dbats_handler *handler);

/** Prepare to iterate over the list of keys ordered by id (which is the same
 *  order the keys were defined).
 *  You must call dbats_walk_keyid_end() before calling dbats_commit(),
 *  dbats_select_time(), or dbats_close().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyid_start(dbats_handler *handler);

/** Get the next key id and key name in the sequence started by
 *  dbats_walk_keyid_start().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[out] key_id_p A pointer to a uint32_t where the key_id will be written.
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written, or NULL if you do not
 *    need the name.
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
extern const dbats_config *dbats_get_config(dbats_handler *handler);

/** Get time series parameters from a DBATS database.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] bid The id of the time series bundle (0 for the primary bundle).
 *  @return pointer to a dbats_bundle_info describing the bundle specified
 *    by bid.
 */
extern const dbats_bundle_info *dbats_get_bundle_info(dbats_handler *handler,
    int bid);

extern void dbats_stat_print(const dbats_handler *handler);
