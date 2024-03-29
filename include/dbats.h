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
 *  - Configure signal handling with dbats_catch_signals().
 *  - Open a database with dbats_open().
 *  - If this is a new database
 *    - define aggregate time series with dbats_aggregate().
 *  - dbats_commit_open().
 *  - If this is a new database
 *    - define keys with dbats_bulk_get_key_id().
 *  - Loop:
 *    - Select a working snapshot with dbats_select_snap().
 *    - Write primary values for multiple keys with dbats_set(), and/or
 *    - read primary and/or aggregate values for multiple keys with dbats_get().
 *    - If dbats_set() deadlocked,
 *      - repeat the loop with the same timestamp and data.
 *    - If another error occured or @ref dbats_caught_signal is set,
 *      - call dbats_abort_snap() and break the loop.
 *    - dbats_commit_snap().
 *    - If dbats_commit_snap() deadlocked,
 *      - repeat the loop with the same timestamp and data.
 *  - Close the database with dbats_close().
 *  - Call dbats_deliver_signal() in case a signal had been caught.
 *
 *  Functions that return an int will return one of the following types of
 *  values:
 *    - 0 for success
 *    - a (positive) E* constant defined in <errno.h> for a system error
 *    - a (negative) DB_* constant defined in <db.h> for a DB error
 *    - -1 for other error
 *
 *  If multiple snapshots (in different processes or threads) are accessing
 *  the same DBATS database concurrently, the writers may deadlock.
 *  When this happens, one or
 *  more operations will be cancelled so that another may proceed.  The
 *  cancelled operations will return DB_LOCK_DEADLOCK.  After a function in a
 *  transaction returns DB_LOCK_DEADLOCK, no other dbats calls are allowed on
 *  that dbats_snapshot (or dbats_handler) until dbats_abort_snap() (or
 *  dbats_abort_open()) is called to clean up the cancelled transaction.  When
 *  an operation needs to be cancelled, one with the lowest priority is
 *  preferred.  There are three priority categories; from lowest to highest,
 *  they are: read only; writable, but haven't yet called dbats_set();
 *  writable, and have called dbats_set().  Within the two writable
 *  categories, priority depends on timestamp, so that inserts of "live" data
 *  have higher priority than inserts of historic data.
 *
 *  Although this API will not prevent you from opening two snapshots for
 *  writing within a single thread, doing so is not recommended since it may
 *  lead to an unbreakable deadlock.
*/

#ifndef DBATS_H
#define DBATS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "dbats_log.h"

/* ************************************************** */

#define DBATS_DB_VERSION     11  ///< Version of db format written by this API

#define DBATS_KEYLEN         1024 ///< max length of key name
#define DBATS_KEY_IS_PREFIX  0x80000000 ///< keyid describes a node, not a key

/** @name Flags */
///@{
#define DBATS_CREATE       0x0001 ///< create object if it doesn't exist
#define DBATS_PRELOAD      0x0004 ///< load fragments when snapshot is selected
#define DBATS_READONLY     0x0008 ///< don't allow writing
#define DBATS_UNCOMPRESSED 0x0010 ///< don't compress written time series data
#define DBATS_EXCLUSIVE    0x0020 ///< obtain exclusive lock on whole db
#define DBATS_NO_TXN       0x0040 ///< don't use transactions (fast but unsafe)
#define DBATS_UPDATABLE    0x0080 ///< allow updates to existing values
#define DBATS_GLOB         0x0200 ///< (internal use)
#define DBATS_DELETE       0x0400 ///< (internal use)
///@}

/** Aggregation function identifiers */
enum dbats_agg_func {
    DBATS_AGG_NONE = -1, ///< unknown
    DBATS_AGG_DATA,      ///< primary data value (not an aggregate)
    DBATS_AGG_MIN,       ///< minimum value
    DBATS_AGG_MAX,       ///< maximum value
    DBATS_AGG_AVG,       ///< average of values (double)
    DBATS_AGG_LAST,      ///< last value (i.e., with greatest timestamp)
    DBATS_AGG_SUM,       ///< sum of values (capped at 2e64-1)
    DBATS_AGG_N          // count of agg function identifiers
};

/// Labels for aggregation functions (e.g. dbats_agg_func_label[@ref DBATS_AGG_MIN] is "min")
extern const char *dbats_agg_func_label[];

/// Time series bundle parameters (read only).
typedef struct {
    uint32_t func;           ///< aggregation function
    uint32_t steps;          ///< # of data points contributing to one agg value
    uint32_t period;         ///< time covered by one value (seconds)
    uint32_t keep;           ///< number of data points to keep (0 means keep all)
} dbats_bundle_info;

/** Configuration parameters (read only).
 *  Fields marked "(C)" are properties of the database and are permanently set
 *  when the database is first created.  Other fields are properites of an
 *  individual dbats_handle, set each time a database is opened, and do not
 *  affect other dbats_handles opened by other threads or processes.
 */
typedef struct {
    uint32_t version;          ///< Version of db format (C)
    uint16_t values_per_entry; ///< Number of dbats_values in an entry (C)
    uint16_t entry_size;       ///< Size of an entry in bytes (C)
    uint32_t period;           ///< Primary sample period in seconds (C)
    uint16_t num_bundles;      ///< Number of time series bundles (C)
    uint8_t readonly;          ///< Disallow writing to db
    uint8_t compress;          ///< Compress data written to db
    uint8_t exclusive;         ///< Obtain exclusive lock on whole db
    uint8_t no_txn;            ///< Don't use transactions
    uint8_t updatable;         ///< Allow updates to existing values
} dbats_config;

/// A value stored in a DBATS database.
typedef union {
    uint64_t u64;  ///< 64-bit unsigned integer
    double d;      ///< double (for DBATS_AGG_AVG)
} dbats_value;

typedef struct dbats_handler dbats_handler; ///< Opaque handle for a DBATS db
typedef struct dbats_snapshot dbats_snapshot; ///< Opaque handle for a DBATS snapshot
typedef struct dbats_keyid_iterator dbats_keyid_iterator; ///< Opaque handle for iterating over keys by id
typedef struct dbats_keytree_iterator dbats_keytree_iterator; ///< Opaque handle for iterating over keys by name

/* ************************************************** */

/** Open an existing or new DBATS database.
 *  Some configuration parameters can be set only when creating a database;
 *  when opening an existing database, those parameters will be silently
 *  ignored.
 *  By default, the database will allow reading data values and inserting new
 *  data values, but not updating existing data values.
 *
 *  If dbats_open() succeeds, the process should not call dbats_open() again
 *  with the same path until the former handle is closed with dbats_close().
 *
 *  Opening a database also starts a transaction that can be used to configure
 *  the database.  When configuration is complete, you must call
 *  dbats_commit_open() to commit the transaction.  Before calling
 *  dbats_commit_open():
 *  - if the database is newly created, no other processes will be able to
 *    access the database
 *  - multiple threads should not attempt to use the @c dbats_handler concurrently
 *  - dbats_select_snap() is not allowed
 *  .
 *  After dbats_commit_open(), any number of threads may call
 *  dbats_select_snap() on the @c dbats_handler.
 *
 *  @param[out] handlerp the address of a dbats_handler*; after the call,
 *    *handlerp will contain a pointer that must be passed to subsequent calls
 *    on this database.
 *  @param[in] path path of an @b existing directory containing a DBATS
 *    database or a @b nonexistant directory in which to create a new DBATS
 *    database.
 *  @param[in] values_per_entry number of dbats_values in the array read by
 *    dbats_get() or written by dbats_set()
 *    (ignored unless creating a new database).
 *  @param[in] period the number of seconds between primary data values
 *    (ignored unless creating a new database).
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    -	DBATS_CREATE - create the database if it doesn't already exist
 *    -	DBATS_READONLY - do not allow writing to the database (improves
 *      performance)
 *    -	DBATS_UNCOMPRESSED - do not compress written time series data
 *    -	DBATS_EXCLUSIVE - do not allow any other process to access the database
 *    -	DBATS_NO_TXN - do not use transactions (faster, but may corrupt the
 *      database if other processes or threads try to access the database or
 *      if the process exits uncleanly)
 *    -	DBATS_UPDATABLE - allow updates to existing values
 *  @param[in] mode permissions for files created by this function (as defined
 *    by open(2)) and modified by the process's umask.  A value of 0 is
 *    treated as 0644. (This function will automatically add write permission
 *    to certain transaction and locking related files as needed to allow
 *    read-only access.)
 *  @return
 *    - 0 for success;
 *    - EINVAL if a parameter was invalid;
 *    - ENOMEM if out of memory;
 *    - ENOENT if the directory specified by @c path does not exist and
 *      @c flags did not contain DBATS_CREATE, or if the directory does exist
 *      but does not contain a valid database after waiting several seconds;
 *    - DB_VERSION_MISMATCH if the version of the BDB library or DBATS library
 *      is not compatible with that used to create the database;
 *    - other nonzero value for other errors.
 */
extern int dbats_open(dbats_handler **handlerp,
    const char *path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags,
    int mode);

/** Commit the transaction started by dbats_open().  This will flush any pending
 *  writes to the database and release any associated database locks and
 *  other resources.
 *  If the database was created in this transaction, no other processes will
 *  be able to access the database until dbats_commit_open() is called.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return
 *    - 0 for success;
 *    - DB_LOCK_DEADLOCK if the operation was cancelled to resolve a deadlock;
 *    - other nonzero value for other errors.
 */
extern int dbats_commit_open(dbats_handler *handler);

/** Abort a transaction begun by dbats_open(),
 *  undoing all changes made during the transaction and
 *  releasing any associated database locks and other resources.  After calling
 *  this function, the dbats_handler is no longer valid.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_abort_open(dbats_handler *handler);

/** Close a database opened by dbats_open().
 *  All snapshots must be committed or aborted before calling dbats_close().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_close(dbats_handler *handler);

/** Defines an aggregate.
 *  Should be called after opening a database with dbats_open() but before
 *  dbats_commit_open(), and before any other process has written
 *  data to the database.  Values for DBATS_AGG_AVG are stored as
 *  @c dbats_value.d; all other aggregates are stored as @c dbats_value.u64.
 *
 *  For example, given a primary data bundle with a period of 60s (1 minute),
 *  <code>dbats_aggregate(handler, DBATS_AGG_MAX, 120)</code> will define an
 *  aggregate bundle that tracks the maximum value seen in each sub-sequence
 *  of 120 primary values (2 hours).
 *
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] func a @ref dbats_agg_func indicating which aggregation
 *    function to apply to the data points.
 *  @param[in] steps number of primary data points to include in an aggregate
 *    point
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_aggregate(dbats_handler *handler, enum dbats_agg_func func, int steps);

/** Look up an aggregate function by name (case insensitive).
 *  @param[in] name the name of an aggregate function
 *  @return the @ref dbats_agg_func function id, or @ref DBATS_AGG_NONE if
 *    no match is found
 */
extern enum dbats_agg_func dbats_find_agg_func(const char *name);

/** Get the earliest timestamp in a time series bundle.
 *  At least one key in the bundle will have a value at the start time.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @param[out] start A pointer to a uint32_t that will be filled in with
 *    the time of the earliest entry in the bundle, or 0 if an error occurred.
 *  @return 0 for success, DB_NOTFOUND if the bundle is empty,
 *    or other nonzero value for error.
 */
extern int dbats_get_start_time(dbats_handler *handler, dbats_snapshot *snap, int bid, uint32_t *start);

/** Get the latest timestamp in a time series bundle.
 *  At least one key in the bundle will have a value at the end time.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @param[out] end A pointer to a uint32_t that will be filled in with
 *    the time of the latest entry in the bundle, or 0 if an error occurred.
 *  @return 0 for success, DB_NOTFOUND if the bundle is empty,
 *    or other nonzero value for error.
 */
extern int dbats_get_end_time(dbats_handler *handler, dbats_snapshot *snap, int bid, uint32_t *end);

/** Configure the number of data points to keep for each time series of a
 *  bundle.
 *  When a transaction started by dbats_select_snap() is committed, any data
 *  points that are <code>keep</code> periods or more older than the latest
 *  point ever set (for any key) will be deleted.
 *  Once a point is deleted for being outside the limit, it can not be set
 *  again, even if the limit is changed.
 *  The limit for the primary data bundle (bid 0) can not be smaller than the
 *  number of steps in the largest aggregate.
 *
 *  For example, if a bundle with period 60s has a keep limit of 1440, values
 *  more than 1 day older than the last timestamp set for any key in the
 *  bundle will be deleted.  If the bundle contains data for keys "foo" and
 *  "bar", and a new value for "foo" is set for 15:37:00 today, then (as soon
 *  as that transaction is committed) all values for both "foo" and "bar" up
 *  to 15:36:00 yesterday will be deleted.
 *
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @param[in] keep The number of data points to keep in each time series of
 *    the bundle, or 0 to keep all points.
 *  @return
 *    - 0 for success;
 *    - DB_NOTFOUND if bid does not refer to a valid bundle;
 *    - EINVAL if the limit is too small;
 *    - other nonzero value for other errors.
 */
extern int dbats_series_limit(dbats_handler *handler, int bid, int keep);

/** Store arbitrary user-defined data on a dbats_handler.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] data a pointer to store on dbats_handler.
 */
extern void dbats_set_userdata(dbats_handler *handler, void *data);

/** Retreive user-defined data from a dbats_handler.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @return the pointer stored by dbats_set_userdata().
 */
extern void *dbats_get_userdata(dbats_handler *handler);

/** Find a bundle covering a time range.
 *  Find the bundle that best covers the time range starting at
 *  <code>start</code>.
 *  Only the primary bundle (0) and bundles with aggregation function
 *  <code>func</code> are considered.
 *  - If one bundle covers significantly less of the time range than another,
 *    the one with better coverage is preferred.
 *  - In the event of a tie above, if either bundle has more than @c max_points
 *    points, the bundle with fewer points is preferred.
 *  - In the event of a tie above, if neither bundle has more than @c max_points
 *    points, the bundle with more points is preferred.
 *
 *  Note that it is possible for the selected bundle to have more than @c
 *  max_points points if there is no other bundle that covers as much of the
 *  time range.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] func aggregation function
 *  @param[in] start beginning of the time range
 *  @param[in] end end of the time range
 *  @param[in] max_points maximum number of data points wanted.  If @c
 *    max_points is 0, it is treated as infinite.
 *  @param[in] exclude the first sample should or should not include the @c from
 *    value if @c exclude is 0 or 1, respectively.
 *  @return the bundle id of the best bundle
 */
extern int dbats_best_bundle(dbats_handler *handler, enum dbats_agg_func func,
    uint32_t start, uint32_t end, int max_points, int exclude);

/** Round a time value down to a multiple of a bundle's period.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @param[in,out] t a unix time value (seconds since 1970-01-01 00:00:00
 *    UTC, without leap seconds).  Value will be rounded down to the nearest
 *    multiple of the data period, counting from 00:00 on the first Sunday of
 *    1970, UTC.
 *  @return the rounded time value (*t)
 */
extern uint32_t dbats_normalize_time(const dbats_handler *handler,
    int bid, uint32_t *t);

/** Create a "snapshot", that is, a working copy of a fixed point in time that
 *  can be manipulated with dbats_get(), dbats_set(), etc.
 *  All operations on this snapshot take place within a transaction; that is,
 *  they will not be visible to other processes/threads until they are
 *  committed with dbats_commit_snap(), or they can be undone with
 *  dbats_abort_snap().
 *  If this function fails for any reason, it returns nonzero without
 *  beginning a transaction.
 *  If multiple snapshots are being used to write to the database in parallel
 *  (in other processes or threads), this function may block
 *  until those other snapshots complete their transactions.
 *  If a deadlock occurs within this function, it will automatically retry, up
 *  to a limit; if this limit is reached, this function will return
 *  DB_LOCK_DEADLOCK without beginning a transaction (so dbats_abort_snap() is
 *  not needed).
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[out] snapp the address of a dbats_snapshot*; after the call, *snapp
 *    will contain a pointer that must be passed to subsequent function calls.
 *  @param[in] time_value the desired time, which will be rounded down by
 *    dbats_normalize_time().
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    -	DBATS_READONLY - do not allow writing to this snapshot, even if the db
 *      was opened for writing (improves performance)
 *    - DBATS_PRELOAD - load data immediately instead of waiting until it's
 *      needed (if you're going to load all the data eventually, this flag can
 *      reduce the chances of deadlock)
 *  @return
 *    - 0 for success;
 *    - EINVAL if the database was not opened with DBATS_READONLY and the
 *      selected time is outside the keep limit set by dbats_series_limit();
 *    - DB_LOCK_DEADLOCK if the operation was cancelled to resolve a deadlock;
 *    - other nonzero value for other errors.
 */
extern int dbats_select_snap(dbats_handler *handler, dbats_snapshot **snapp,
    uint32_t time_value, uint32_t flags);

/** Commit the transaction started by dbats_select_snap().  This will flush
 *  any pending writes to the database and release any associated database
 *  locks and other resources.
 *  Any data points that fall outside the limit set by dbats_series_limit()
 *  will be deleted.
 *  After calling this function, the snapshot is no longer valid.
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @return
 *    - 0 for success;
 *    - DB_LOCK_DEADLOCK if the operation was cancelled to resolve a deadlock;
 *    - other nonzero value for other errors.
 */
extern int dbats_commit_snap(dbats_snapshot *snap);

/** Abort a transaction begun by dbats_select_snap(), undoing all changes made
 *  during the transaction and releasing any associated database locks and
 *  other resources.  After calling this function, the snapshot is no longer
 *  valid.
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @return 0 for success, nonzero for error.
 */
#define dbats_abort_snap(snap) (dbats_abort_snap)(__FILE__, __LINE__, snap)
/// @cond UNDOCUMENTED
extern int (dbats_abort_snap)(const char *fname, int line, dbats_snapshot *ds);
/// @endcond

/** Get the id of a new or existing key.
 *  The key must already exist unless the DBATS_CREATE flag is given.
 *  When getting or creating multiple keys, it is faster to do it with
 *  dbats_bulk_get_key_id().
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[in] key the name of the key.
 *  @param[out] key_id_p a pointer to a uint32_t where the id for key will be
 *    written.
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    - DBATS_CREATE - create the key if it does not already exist.
 *  @return 0 for success, DB_NOTFOUND if the key does not exist and
 *    DBATS_CREATE flag was not set, or other nonzero value for error.
 */
extern int dbats_get_key_id(dbats_handler *handler, dbats_snapshot *snap,
    const char *key, uint32_t *key_id_p, uint32_t flags);

/** Get the ids of a large number of new or existing keys.
 *  The keys must already exist unless the DBATS_CREATE flag is given.
 *  When creating a large number (> 20000) of keys, it is recommended to
 *  do so outside of a transaction to avoid excessive locking (this function
 *  will create keys in efficiently-sized batches and use its own transactions).
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[in,out] *n_keys_p On input, should point to an integer containing
 *    the number of keys in the @c keys array.  On output, that integer will
 *    contain the number of ids written into @c key_ids.  If this function
 *    returns 0, the output value of *n_keys_p will equal the input value;
 *    otherwise, the output value may be anywhere between 0 and the input
 *    value, indicating the number of keys that were successfully looked up or
 *    created before the error occurred.
 *  @param[in] keys an array of key names
 *  @param[out] key_ids an array of uint32_t where the ids for
 *    keys will be written.
 *  @param[in] flags a bitwise-OR combination of any of the following:
 *    - DBATS_CREATE - create any key that does not already exist.
 *  @return 0 for success, DB_NOTFOUND if any key does not exist and
 *    DBATS_CREATE flag was not set, or other nonzero value for error.
 */
extern int dbats_bulk_get_key_id(dbats_handler *handler, dbats_snapshot *snap,
    uint32_t *n_keys_p, const char * const *keys, uint32_t *key_ids,
    uint32_t flags);

/** Get the name of an existing key.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[in] key_id the id of the key.
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_get_key_name(dbats_handler *handler, dbats_snapshot *snap,
    uint32_t key_id, char *namebuf);

/** For each metric key that matches pattern, delete the key and all its data.
 *  There must not be a snapshot open when this function is called.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] pattern A fileglob-like pattern as described under
 *    dbats_glob_keyname_start().
 *  @return 0 for success, EINVAL if the pattern is invalid, or other
 *    nonzero value for other error.
 */
extern int dbats_delete_keys(dbats_handler *dh, const char *pattern);

/** Write a value to the primary time series for the specified key and
 *  snapshot (time).
 *  All aggregate values whose time ranges contain this primary data point
 *  will also be updated.
 *  Note that values are not guaranteed to be safely stored in the database
 *  until dbats_commit_snap() is called and returns successfully.
 *  In particular, when multiple processes are writing to the database,
 *  there is a non-negligible probability of losing data due to deadlock in
 *  dbats_set() or dbats_commit_snap().  You are advised to hold on to a copy
 *  of your original data until dbats_commit_snap() succeeds, so that you can
 *  repeat dbats_select_snap() and dbats_set() if needed.
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[in] key_id the id of the key.
 *  @param[in] valuep a pointer to an array of values_per_entry dbats_value
 *    to be written to the database.  Each dbats_value.u64 should contain a
 *    value.
 *  @return
 *    - 0 for success;
 *    - EPERM if the handler is not open or was opened with DBATS_READONLY;
 *    - DB_KEYEXIST if a value already exists for the given time and key, and
 *      handler was not opened with DBATS_UPDATABLE;
 *    - ENOSYS if a value already exists for the given time and key, and an
 *      aggregate depending on the value has the function DBATS_AGG_MIN or
 *      DBATS_AGG_MAX;
 *    - DB_LOCK_DEADLOCK if the operation was cancelled to resolve a deadlock;
 *    - other nonzero value for other errors.
 */
extern int dbats_set(dbats_snapshot *snap, uint32_t key_id,
    const dbats_value *valuep);

/** Write a value to the primary time series for the specified key and the
 *  time selected by dbats_select_snap().
 *  Equivalent to @ref dbats_get_key_id (handler, key, &key_id, flags)
 *  followed by @ref dbats_set (handler, key_id, valuep).
 *  Note that dbats_set() is much faster than dbats_set_by_key() if you know
 *  the key_id already.
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
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
 *    - DB_LOCK_DEADLOCK if the operation was cancelled to resolve a deadlock;
 *    - other nonzero value for other errors.
 */
extern int dbats_set_by_key (dbats_snapshot *snap, const char *key,
    const dbats_value *valuep, int flags);

/** Read an entry (array of dbats_value) from the database for the specified
 *  bundle id and key and the time selected by dbats_select_snap().
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[in] key_id the id of the key.
 *  @param[out] valuepp the address of a dbats_value*; after the call,
 *    *valuepp will point to an array of values_per_entry dbats_values
 *    read from the database.
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @return
 *    - 0 for success;
 *    - DB_NOTFOUND if the key does not exist;
 *    - DB_LOCK_DEADLOCK if the operation was cancelled to resolve a deadlock;
 *    - other nonzero value for other errors.
 */
extern int dbats_get(dbats_snapshot *snap, uint32_t key_id,
    const dbats_value **valuepp, int bid);

/** Read an array of dbats_values from the database for the specified bundle
 *  id and key and the time selected by dbats_select_snap().
 *  Equivalent to dbats_get_key_id() followed by dbats_get().
 *  Note that dbats_get() is significantly faster if you already
 *  know the key_id.
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[in] key the name of the key.
 *  @param[out] valuepp the address of a dbats_value*; after the call,
 *    *valuepp will point to an array of values_per_entry dbats_values
 *    read from the database.
 *  @param[in] bid bundle id (0 for the primary bundle).
 *  @return
 *    - 0 for success;
 *    - DB_NOTFOUND if the key does not exist;
 *    - DB_LOCK_DEADLOCK if the operation was cancelled to resolve a deadlock;
 *    - other nonzero value for other errors.
 */
extern int dbats_get_by_key(dbats_snapshot *snap, const char *key,
    const dbats_value **valuepp, int bid);

/** Get the number of keys in the database.
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[out] num_keys A pointer to a uint32_t that will be filled in with
 *    the number of keys in the databasse.
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_num_keys(dbats_handler *handler, uint32_t *num_keys);

/** Prepare to iterate over the list of keys and keytree nodes that match
 *  pattern, ordered by name.
 *  If pattern is NULL, all keys (but not nodes) will be iterated over.
 *  Otherwise, keys and nodes matching the pattern will be iterated over.
 *  The pattern is similar to shell filename globbing, except that
 *  hierarchical components are separated by '.' instead of '/'.
 *    - * matches any zero or more characters (except '.')
 *    - ? matches any one character (except '.')
 *    - [...] matches any one character in the character class (except '.')
 *      - A leading '^' negates the character class
 *      - Two characters separated by '-' matches any ASCII character between
 *        the two characters, inclusive
 *    - {...} matches any one string of characters in the comma-separated list
 *      of strings
 *    - Any other character matches itself.
 *    - Any special character can have its special meaning removed by
 *      preceeding it with '\'.
 *
 *  Example:
 *  \code
 *  dbats_handler *handler;
 *  dbats_snapshot *snap;
 *  dbats_keytree_iterator *dki;
 *  int rc;
 *  uint32_t keyid;
 *  char name[DBATS_KEYLEN];
 *  ...
 *  rc = dbats_glob_keyname_start(handler, snap, &dki, "*.*.uniq_*_ip");
 *  if (rc != 0) goto error_handling;
 *  while (dbats_glob_keyname_next(dki, &keyid, name) == 0 && !dbats_caught_signal) {
 *      if (keyid & DBATS_KEY_IS_PREFIX) {
 *          // do something with node described by keyid and name
 *      } else {
 *          // do something with key described by keyid and name
 *      }
 *  }
 *  dbats_glob_keyname_end(dki);
 *  \endcode
 *
 *  If @c snap is not NULL, the operations will occur within
 *  @c snap's transaction; otherwise, if @c handler's transaction has not been
 *  closed, they will occur within @c handler's transaction; otherwise, this
 *  function will begin a new transaction which will be committed by
 *  dbats_glob_keyname_end().
 *
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[out] dkip the address of a dbats_keytree_iterator*; after the call,
 *    *dkip will contain a pointer that must be passed to
 *    subsequent calls of dbats_glob_keyname_next() and
 *    dbats_glob_keyname_end().
 *  @param[in] pattern A fileglob-like pattern
 *  @return 0 for success, or EINVAL if the pattern is invalid.
 */
extern int dbats_glob_keyname_start(dbats_handler *handler,
    dbats_snapshot *snap, dbats_keytree_iterator **dkip,
    const char *pattern);

/** Get the next key id and key name in the sequence started by
 *  dbats_glob_keyname_start().
 *  @param[in] dki A dbats_keytree_iterator created by
 *    dbats_glob_keyname_start().
 *  @param[out] key_id_p A pointer to a uint32_t where the key_id will be written.
 *    If (keyid & DBATS_KEY_IS_PREFIX) is true, the result describes a keytree
 *    node; otherwise, it describes a keytree leaf (i.e., an actual key).
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written, or NULL if you do not
 *    need the name.
 *  @return
 *    - 0 for success;
 *    - DB_NOTFOUND if there are no more matching keys;
 *    - other nonzero value for error.
 */
extern int dbats_glob_keyname_next(dbats_keytree_iterator *dki,
    uint32_t *key_id_p, char *namebuf);

/** End the sequence started by dbats_glob_keyname_start().
 *  @param[in] dki A dbats_keytree_iterator created by
 *    dbats_glob_keyname_start().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_glob_keyname_end(dbats_keytree_iterator *dki);

/** Prepare to iterate over the list of keys ordered by id (which is the same
 *  order the keys were defined).
 *  If @c snap is not NULL, the operations will occur within
 *  @c snap's transaction; otherwise they will occur within
 *  @c handler's transaction.
 *  You must call dbats_walk_keyid_end() before committing the transaction.
 *
 *  Example:
 *  \code
 *  dbats_handler *handler;
 *  dbats_snapshot *snap;
 *  dbats_keyid_iterator *dki;
 *  int rc;
 *  uint32_t keyid;
 *  char name[DBATS_KEYLEN];
 *  ...
 *  rc = dbats_walk_keyid_start(handler, snap, &dki);
 *  if (rc != 0) goto error_handling;
 *  while (dbats_walk_keyid_next(dki, &keyid, name) == 0 && !dbats_caught_signal) {
 *      // do something with keyid and/or name
 *  }
 *  dbats_walk_keyid_end(dki);
 *  \endcode
 *
 *  @param[in] handler A dbats_handler created by dbats_open().
 *  @param[in] snap A dbats_snapshot created by dbats_select_snap().
 *  @param[out] dkip the address of a dbats_keyid_iterator*; after the call,
 *    *dkip will contain a pointer that must be passed to
 *    subsequent calls of dbats_walk_keyid_next() and dbats_walk_keyid_end().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyid_start(dbats_handler *handler,
    dbats_snapshot *snap, dbats_keyid_iterator **dkip);

/** Get the next key id and key name in the sequence started by
 *  dbats_walk_keyid_start().
 *  @param[in] dki A dbats_keyid_iterator created by dbats_walk_keyid_start().
 *  @param[out] key_id_p A pointer to a uint32_t where the key_id will be written.
 *  @param[out] namebuf a pointer to an array of at least DBATS_KEYLEN
 *    characters where the key's name will be written, or NULL if you do not
 *    need the name.
 *  @return
 *    - 0 for success;
 *    - DB_NOTFOUND if there are no more keys;
 *    - other nonzero value for error.
 */
extern int dbats_walk_keyid_next(dbats_keyid_iterator *dki, uint32_t *key_id_p,
    char *namebuf);

/** End the sequence started by dbats_walk_keyid_start().
 *  @param[in] dki A dbats_keyid_iterator created by dbats_walk_keyid_start().
 *  @return 0 for success, nonzero for error.
 */
extern int dbats_walk_keyid_end(dbats_keyid_iterator *dki);

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


/** @name signal handling convenience functions */
///@{

/** The last signal caught by the DBATS signal handler, or 0 if no signal has
 *  been caught.
 */
extern int dbats_caught_signal;

/** If signal @c sig is currently handled by SIG_DFL, change it to a
 *  DBATS signal handler that sets @c dbats_caught_signal.
 *  @param[in] sig the signal to handle
 */
extern void dbats_catch_signal(int sig);

/** Shorthand for calling dbats_catch_signal() on the following signals
 *  that are normally fatal:
 *  @c SIGINT, @c SIGHUP, @c SIGTERM, and @c SIGPIPE.
 */
extern void dbats_catch_signals(void);

/** If signal @c sig was configured with dbats_catch_signal(), reset it to
 *  SIG_DFL.
 *  @param[in] sig the signal to handle
 */
extern void dbats_restore_signal(int sig);

/** If any signal configured with dbats_catch_signal() has been caught since
 *  the handler was set, restore the SIG_DFL handler and @c raise the signal.
 *  This is useful @b after cleanly aborting transactions and closing the
 *  database, to exit the process with the same status that would have occurred
 *  if the signal had not been caught.
 */
extern void dbats_deliver_signal(void);

///@}

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif // DBATS_H
