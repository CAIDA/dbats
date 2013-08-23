/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lessed General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 */

#include <stdio.h>
#include <db.h>
#include <sys/stat.h>
#include <errno.h>

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/types.h>
#include <db.h>
#include <assert.h>
#include <arpa/inet.h> // htonl() etc

#include "dbats.h"
#include "quicklz.h"

// Limits
#define ENTRIES_PER_FRAG    10000 // number of entries in a fragment
#define MAX_NUM_FRAGS       16384 // max number of fragments in a tslice
#define MAX_NUM_SERIES         16 // max number of time series

#define HAVE_SET_LK_EXCLUSIVE 0

/*************************************************************************/
// compile-time assertion (works anywhere a declaration is allowed)
#define ct_assert(expr, label)  enum { ASSERT_ERROR__##label = 1/(!!(expr)) };

ct_assert((sizeof(double) <= sizeof(dbats_value)), dbats_value_is_smaller_than_double)

/*************************************************************************/

// bit vector
#define vec_size(N)       (((N)+7)/8)                    // size for N bits
#define vec_set(vec, i)   (vec[(i)/8] |= (1<<((i)%8)))   // set the ith bit
#define vec_reset(vec, i) (vec[(i)/8] &= ~(1<<((i)%8)))  // reset the ith bit
#define vec_test(vec, i)  (vec && (vec[(i)/8] & (1<<((i)%8)))) // test ith bit

#define valueptr(dh, sid, frag_id, offset) \
    ((dbats_value*)(&dh->tslice[sid]->frag[frag_id]->data[offset * dh->cfg.entry_size]))

// "Fragment", containing a large subset of entries for a timeslice
typedef struct dbats_frag {
    uint8_t compressed; // is data in db compressed?
    uint8_t data[]; // values (C99 flexible array member)
} dbats_frag;

#define keyfrag(keyid)     ((keyid) / ENTRIES_PER_FRAG)
#define keyoff(keyid)      ((keyid) % ENTRIES_PER_FRAG)

// Logical row or "time slice", containing all the entries for a given time
// (broken into fragments).
typedef struct dbats_tslice {
    uint32_t time;                        // start time (unix seconds)
    uint32_t num_frags;                   // number of fragments
    uint8_t *is_set[MAX_NUM_FRAGS];       // bitvectors: which cols have values
    dbats_frag *frag[MAX_NUM_FRAGS];      // blocks of values
    uint8_t frag_changed[MAX_NUM_FRAGS];  // which fragments have changed
} dbats_tslice;

#define fragsize(dh) \
    (sizeof(dbats_frag) + ENTRIES_PER_FRAG * (dh)->cfg.entry_size)

typedef struct {
    uint32_t sid;
    uint32_t time;
    uint32_t frag_id;
} fragkey_t;

#define N_TXN 16

struct dbats_handler {
    dbats_config cfg;
    const char *path;          // path of BDB environment directory
    uint8_t is_open;
    uint8_t preload;           // load frags when tslice is selected
    uint8_t serialize;         // allow only one writer at a time
    void *state_compress;
    void *state_decompress;
    DB_ENV *dbenv;             // DB environment
    DB *dbMeta;                // config parameters
    DB *dbKeyname;             // key name -> key id
    DB *dbKeyid;               // recno -> key name (note: keyid == recno - 1)
    DB *dbData;                // {sid, time, frag_id} -> value fragment
    DB *dbIsSet;               // {sid, time, frag_id} -> is_set fragment
    DBC *keyname_cursor;       // for iterating over key names
    DBC *keyid_cursor;         // for iterating over key ids
    dbats_tslice **tslice;     // a tslice for each time series
    dbats_series_info *series; // parameters for each time series
    uint8_t *db_get_buf;       // buffer for data fragment
    size_t db_get_buf_len;
    DB_TXN *txn[N_TXN];        // stack of transactions ([0] is NULL)
    int n_txn;                 // number of transactions in stack (not incl [0])
    int n_internal_txn;        // number of internal transactions in stack
    uint32_t active_start;     // first timestamp in active period
                               // (corresponding to is_set[0])
    uint32_t active_end;       // last timestamp in active period
    uint32_t active_last_data; // last timestamp in active period with data
    uint8_t ***is_set;         // each is_set[timeindex][fragid] is a bitvector
                               // indicating which cols have values
};

#define current_txn(dh) ((dh)->txn[(dh)->n_txn])

const char *dbats_agg_func_label[] = {
    "data", "min", "max", "avg", "last", "sum"
};

/************************************************************************
 * utilities
 ************************************************************************/

// Construct a DBT for sending data to DB.
#define DBT_in(_var, _data, _size) \
    DBT _var; \
    memset(&_var, 0, sizeof(DBT)); \
    _var.data = _data; \
    _var.size = _size; \
    _var.flags = DB_DBT_USERMEM

// Construct a DBT for receiving data from DB.
#define DBT_out(_var, _data, _ulen) \
    DBT _var; \
    memset(&_var, 0, sizeof(DBT)); \
    _var.data = _data; \
    _var.ulen = _ulen; \
    _var.flags = DB_DBT_USERMEM

// Construct a DBT for receiving nothing from DB.
#define DBT_null(_var) \
    DBT _var; \
    memset(&_var, 0, sizeof(DBT));

static inline uint32_t min(uint32_t a, uint32_t b) { return a < b ? a : b; }
static inline uint32_t max(uint32_t a, uint32_t b) { return a > b ? a : b; }

// ceiling of division
#define div_ceil(n, d) (((n) + (d) - 1) / (d))

#if 1
static void *emalloc(size_t sz, const char *msg)
{
    void *ptr = malloc(sz);
    if (!ptr) {
	int err = errno;
	dbats_log(LOG_WARNING, "Can't allocate %zu bytes for %s", sz, msg);
	errno = err;
    }
    return ptr;
}

static void *ecalloc(size_t n, size_t sz, const char *msg)
{
    void *ptr = calloc(n, sz);
    if (!ptr) {
	int err = errno;
	dbats_log(LOG_WARNING, "Can't alloc %zu*%zu bytes for %s", n, sz, msg);
	errno = err;
    }
    return ptr;
}
#else
# define emalloc(size, msg) xmalloc(size)
# define ecalloc(n, size, msg) xmalloc(n, size)
#endif

/************************************************************************
 * DB wrappers
 ************************************************************************/

static inline int begin_transaction(dbats_handler *dh, const char *name)
{
    if (dh->cfg.no_txn) return 0;
    dbats_log(LOG_FINE, "begin txn: %s", name);
    int rc;
    DB_TXN *parent = current_txn(dh);
    dh->n_txn++;
    if (dh->n_txn > N_TXN) {
	dbats_log(LOG_ERROR, "begin txn: %s: too many txns", name);
	exit(-1);
    }
    DB_TXN **childp = &current_txn(dh);
    rc = dh->dbenv->txn_begin(dh->dbenv, parent, childp, DB_TXN_BULK);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "begin txn: %s: %s", name, db_strerror(rc));
	dh->n_txn--;
	return rc;
    }
    (*childp)->set_name(*childp, name);
    return 0;
}

static inline int commit_transaction(dbats_handler *dh)
{
    if (!dh->n_txn) return 0;
    int rc;
    DB_TXN *txn = current_txn(dh);
    dh->n_txn--;
    const char *name;
    txn->get_name(txn, &name);
    dbats_log(LOG_FINE, "commit txn: %s", name);
    if ((rc = txn->commit(txn, 0)) != 0)
	dbats_log(LOG_ERROR, "commit txn: %s: %s", name, db_strerror(rc));
    rc = dh->dbenv->txn_checkpoint(dh->dbenv, 256, 0, 0);
    if (rc != 0)
	dbats_log(LOG_ERROR, "txn_checkpoint: %s", db_strerror(rc));
    return rc;
}

static int abort_transaction(dbats_handler *dh)
{
    if (!dh->n_txn) return 0;
    int rc;
    DB_TXN *txn = current_txn(dh);
    dh->n_txn--;
    const char *name;
    txn->get_name(txn, &name);
    dbats_log(LOG_FINE, "abort txn: %s", name);
    if ((rc = txn->abort(txn)) != 0)
	dbats_log(LOG_ERROR, "abort txn: %s: %s", name, db_strerror(rc));
    return rc;
}

static int raw_db_open(dbats_handler *dh, DB **dbp, const char *name,
    DBTYPE dbtype, uint32_t flags, int mode)
{
    int rc;

    if ((rc = db_create(dbp, dh->dbenv, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error creating DB dh %s/%s: %s",
	    dh->path, name, db_strerror(rc));
	return rc;
    }

    if (dh->cfg.exclusive) {
#if HAVE_SET_LK_EXCLUSIVE
	if ((rc = (*dbp)->set_lk_exclusive(*dbp, 1)) != 0) {
	    dbats_log(LOG_ERROR, "Error obtaining exclusive lock on %s/%s: %s",
		dh->path, name, db_strerror(rc));
	    return rc;
	}
#endif
    }

    uint32_t dbflags = 0;
    if (!dh->cfg.exclusive)
	dbflags |= DB_THREAD;
    if (flags & DBATS_CREATE)
	dbflags |= DB_CREATE;
    if (flags & DBATS_READONLY)
	dbflags |= DB_RDONLY;

    rc = (*dbp)->open(*dbp, current_txn(dh), name, NULL, dbtype, dbflags, mode);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s/%s (mode=%o): %s",
	    dh->path, name, mode, db_strerror(rc));
	(*dbp)->close(*dbp, 0);
	return rc;
    }
    return 0;
}

static int raw_db_set(dbats_handler *dh, DB *db,
    const void *key, uint32_t key_len,
    const void *value, uint32_t value_len)
{
    int rc;

    if (db == dh->dbData) {
	fragkey_t *fragkey = (fragkey_t*)key;
	dbats_log(LOG_FINEST, "raw_db_set Data: series=%d t=%u frag=%u, "
	    "compress=%d, len=%u", fragkey->sid, fragkey->time,
	    fragkey->frag_id, *(uint8_t*)value, value_len);
    }

    if (dh->cfg.readonly) {
	const char *dbname;
	db->get_dbname(db, &dbname, NULL);
	dbats_log(LOG_WARNING, "Unable to set value in database \"%s\" "
	    "(read-only mode)", dbname);
	return EPERM;
    }

    DBT_in(dbt_key, (void*)key, key_len);
    DBT_in(dbt_data, (void*)value, value_len);

    if ((rc = db->put(db, current_txn(dh), &dbt_key, &dbt_data, 0)) != 0) {
	const char *dbname;
	db->get_dbname(db, &dbname, NULL);
	dbats_log(LOG_WARNING, "Error in raw_db_set: %s: %s",
	    dbname, db_strerror(rc));
	return rc;
    }
    return 0;
}

#define QLZ_OVERHEAD 400

// Get a value from the database.
// Before the call, *value_len must contain the length of the memory pointed
// to by value.
// After the call, *value_len will contain the length of data written into
// the memory pointed to by value.
static int raw_db_get(dbats_handler *dh, DB* db,
    const void *key, uint32_t key_len, void *value, size_t *value_len,
    uint32_t dbflags)
{
    int rc;

    DBT_in(dbt_key, (void*)key, key_len);
    DBT_out(dbt_data, value, *value_len);
    rc = db->get(db, current_txn(dh), &dbt_key, &dbt_data, dbflags);
    if (rc == 0) {
	*value_len = dbt_data.size;
	return 0;
    }
    if (rc != DB_NOTFOUND)
	dbats_log(LOG_FINE, "raw_db_get failed: %s", db_strerror(rc));
    if (rc == DB_BUFFER_SMALL) {
	dbats_log(LOG_WARNING, "raw_db_get: had %" PRIu32 ", needed %" PRIu32,
	    dbt_data.ulen, dbt_data.size);
    }
    return rc;
}

/*
static void raw_db_delete(const dbats_handler *dh, DB *db,
    const void *key, uint32_t key_len)
{
    if (dh->cfg.readonly) {
	dbats_log(LOG_WARNING, "Unable to delete value (read-only mode)");
	return EPERM;
    }

    DBT_in(dbt_key, (void*)key, key_len);
    if (db->del(db, NULL, &dbt_key, 0) != 0)
	dbats_log(LOG_WARNING, "Error while deleting key");
}
*/

/*************************************************************************/

#define series_info_key(dh, keybuf, sid) \
    sprintf(keybuf, dh->cfg.version < 3 ? "agg%d" : "series%d", sid)

// Warning: never open the same db more than once in the same process, because
// there must be only one DB_ENV per db per process.
// http://docs.oracle.com/cd/E17076_02/html/programmer_reference/transapp_app.html
dbats_handler *dbats_open(const char *path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags)
{
    dbats_log(LOG_CONFIG, "%s", db_full_version(NULL, NULL, NULL, NULL, NULL));

    int rc;
    int mode = 00666;
    uint32_t dbflags = DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG |
	DB_INIT_TXN | DB_THREAD | DB_REGISTER | DB_RECOVER | DB_CREATE;
    int is_new = 0;

    dbats_handler *dh = ecalloc(1, sizeof(dbats_handler), "dh");
    if (!dh) return NULL;
    dh->cfg.readonly = !!(flags & DBATS_READONLY);
    dh->cfg.updatable = !!(flags & DBATS_UPDATABLE);
    dh->cfg.compress = !(flags & DBATS_UNCOMPRESSED);
    dh->cfg.exclusive = !!(flags & DBATS_EXCLUSIVE);
    dh->cfg.no_txn = !!(flags & DBATS_NO_TXN);
    dh->cfg.values_per_entry = 1;
    dh->serialize = 1;
    dh->path = path;

    if ((rc = db_env_create(&dh->dbenv, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error creating DB env: %s",
	    db_strerror(rc));
	return NULL;
    }

    dh->dbenv->set_errpfx(dh->dbenv, path);
    dh->dbenv->set_errfile(dh->dbenv, stderr);

    dh->dbenv->set_verbose(dh->dbenv,
	DB_VERB_FILEOPS | DB_VERB_RECOVERY | DB_VERB_REGISTER, 1);
    dh->dbenv->set_msgfile(dh->dbenv, stderr);

    if (flags & DBATS_CREATE) {
	if (dh->cfg.readonly) {
	    dbats_log(LOG_ERROR,
		"DBATS_CREATE and DBATS_READONLY are not compatible");
	    return NULL;
	} else if (mkdir(path, 0777) == 0) {
	    // new database dir
	    is_new = 1;
	    dbflags |= DB_CREATE;
	} else if (errno == EEXIST) {
	    // existing database dir
	    flags &= ~DBATS_CREATE;
	    // Wait for $path/meta to exist to avoid race condition with
	    // another process creating a dbats environment.
	    char filename[PATH_MAX];
	    struct stat statbuf;
	    int timeout = 10;
	    sprintf(filename, "%s/meta", path);
	    while (stat(filename, &statbuf) != 0) {
		if (errno != ENOENT) {
		    dbats_log(LOG_ERROR, "Error checking %s: %s",
			filename, strerror(errno));
		    return NULL;
		}
		if (--timeout <= 0) {
		    dbats_log(LOG_ERROR, "Timeout waiting for %s", filename);
		    return NULL;
		}
		sleep(1);
	    }
	} else {
	    // new database dir failed
	    dbats_log(LOG_ERROR, "Error creating %s: %s",
		path, strerror(errno));
	    return NULL;
	}
    }

    if (is_new) {
	char filename[PATH_MAX];
	sprintf(filename, "%s/DB_CONFIG", path);
	FILE *file = fopen(filename, "w");
	if (!file) {
	    dbats_log(LOG_ERROR, "Error opening %s: %s",
		filename, strerror(errno));
	    return NULL;
	}

	fprintf(file, "log_set_config DB_LOG_AUTO_REMOVE on\n");

	// XXX These should be based on max expected number of metric keys?
	// 40000 is enough to insert at least 2000000 metric keys.
	fprintf(file, "set_lk_max_locks 40000\n");
	fprintf(file, "set_lk_max_objects 40000\n");

	fclose(file);
    }

    if ((rc = dh->dbenv->open(dh->dbenv, path, dbflags, mode)) != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s: %s", path, db_strerror(rc));
	return NULL;
    }

    if (!HAVE_SET_LK_EXCLUSIVE && dh->cfg.exclusive) {
	// emulate exclusive lock with an outer txn
	if (begin_transaction(dh, "exclusive txn") != 0) return NULL;
	dh->n_internal_txn++;
    }
    if (begin_transaction(dh, "open txn") != 0) goto abort;

#define open_db(db,name,type) raw_db_open(dh, &dh->db, name, type, flags, mode)
    if (open_db(dbMeta,    "meta",    DB_BTREE) != 0) goto abort;
    if (open_db(dbKeyname, "keyname", DB_BTREE) != 0) goto abort;
    if (open_db(dbKeyid,   "keyid",   DB_RECNO) != 0) goto abort;
    if (open_db(dbData,    "data",    DB_BTREE) != 0) goto abort;
    if (open_db(dbIsSet,   "is_set",  DB_BTREE) != 0) goto abort;
#undef open_db

    dh->cfg.entry_size = sizeof(dbats_value); // for db_get_buf_len

#define initcfg(field, defaultval) \
    do { \
	size_t value_len = sizeof(dh->cfg.field); \
	if (is_new) { \
	    dh->cfg.field = (defaultval); \
	    if (raw_db_set(dh, dh->dbMeta, #field, strlen(#field), \
		&dh->cfg.field, value_len) != 0) \
		    goto abort; \
	} else { \
	    int writelock = !dh->cfg.readonly || dh->cfg.exclusive; \
	    if (raw_db_get(dh, dh->dbMeta, #field, strlen(#field), \
		&dh->cfg.field, &value_len, writelock ? DB_RMW : 0) != 0) \
	    { \
		dbats_log(LOG_ERROR, "%s: missing config: %s", path, #field); \
		goto abort; \
	    } \
	} \
	dbats_log(LOG_CONFIG, "cfg: %s = %u", #field, dh->cfg.field); \
    } while (0)

    initcfg(version,           DBATS_DB_VERSION);
    initcfg(period,            period);
    initcfg(values_per_entry,  values_per_entry);
    initcfg(num_series,        1);

#undef initcfg

    if (dh->cfg.version != DBATS_DB_VERSION) {
	dbats_log(LOG_ERROR, "database version %d != library version %d",
	    dh->cfg.version, DBATS_DB_VERSION);
	return NULL;
    }

    if (dh->cfg.num_series > MAX_NUM_SERIES) {
	dbats_log(LOG_ERROR, "num_series %d > %d", dh->cfg.num_series,
	    MAX_NUM_SERIES);
	return NULL;
    }

    dh->series = emalloc(MAX_NUM_SERIES * sizeof(*dh->series), "dh->series");
    if (!dh->series) return NULL;
    dh->tslice = emalloc(MAX_NUM_SERIES * sizeof(*dh->tslice), "dh->tslice");
    if (!dh->tslice) return NULL;

    dh->cfg.entry_size = dh->cfg.values_per_entry * sizeof(dbats_value);

    // load metadata for time series
    if (is_new) {
	memset(&dh->series[0], 0, sizeof(dbats_series_info));
	dh->series[0].func = DBATS_AGG_NONE;
	dh->series[0].steps = 1;
	dh->series[0].period = dh->cfg.period;
    } else {
	size_t value_len;
	char keybuf[32];
	for (int sid = 0; sid < dh->cfg.num_series; sid++) {
	    series_info_key(dh, keybuf, sid);
	    value_len = sizeof(dh->series[sid]);
	    if ((rc = raw_db_get(dh, dh->dbMeta, keybuf, strlen(keybuf),
		&dh->series[sid], &value_len, 0)) != 0)
	    {
		dbats_log(LOG_ERROR, "error reading %s: %s",
		    keybuf, db_strerror(rc));
		return NULL;
	    } else if (value_len != sizeof(dbats_series_info)) {
		dbats_log(LOG_ERROR, "corrupt %s: size %zu != %zu",
		    keybuf, value_len, sizeof(dbats_series_info));
		return NULL;
	    }
	}
    }

    // allocate tslice for each time series
    for (int sid = 0; sid < dh->cfg.num_series; sid++) {
	if (!(dh->tslice[sid] = ecalloc(1, sizeof(dbats_tslice), "tslice")))
	    return NULL;
    }

    if (!dh->cfg.readonly) {
	if (dh->cfg.compress) {
	    dh->state_compress = ecalloc(1, sizeof(qlz_state_compress),
		"qlz_state_compress");
	    if (!dh->state_compress) return NULL;
	}

	rc = raw_db_set(dh, dh->dbMeta, "series0", strlen("series0"),
	    &dh->series[0], sizeof(dbats_series_info));
	if (rc != 0) goto abort;
    }

    dh->is_open = 1;
    return dh;

abort:
    abort_transaction(dh);
    return NULL;
}

int dbats_aggregate(dbats_handler *dh, int func, int steps)
{
    int rc;

    DB_BTREE_STAT *stats;
    rc = dh->dbIsSet->stat(dh->dbIsSet, current_txn(dh), &stats, DB_FAST_STAT);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error getting is_set stats: %s", db_strerror(rc));
	return rc;
    }
    int empty = (stats->bt_pagecnt > stats->bt_empty_pg);
    free(stats);

    if (!empty) {
	dbats_log(LOG_ERROR,
	    "Adding a new aggregation to existing data is not yet supported.");
	return -1;
	// If we're going to allow this, we have to make all references to
	// series[] and num_series read from db instead of memory.
    }

    if (dh->cfg.num_series >= MAX_NUM_SERIES) {
	dbats_log(LOG_ERROR, "Too many time series (%d)", MAX_NUM_SERIES);
	return ENOMEM;
    }

    int sid = dh->cfg.num_series++;

    if (!(dh->tslice[sid] = ecalloc(1, sizeof(dbats_tslice), "tslice")))
	return errno ? errno : ENOMEM;

    memset(&dh->series[sid], 0, sizeof(dbats_series_info));
    dh->series[sid].func = func;
    dh->series[sid].steps = steps;
    dh->series[sid].period = dh->series[0].period * steps;

    char keybuf[32];
    series_info_key(dh, keybuf, sid);
    rc = raw_db_set(dh, dh->dbMeta, keybuf, strlen(keybuf),
	&dh->series[sid], sizeof(dbats_series_info));
    if (rc != 0) return rc;
    rc = raw_db_set(dh, dh->dbMeta, "num_series", strlen("num_series"),
	&dh->cfg.num_series, sizeof(dh->cfg.num_series));
    if (rc != 0) return rc;

    return 0;
}

// "DELETE FROM db WHERE db.sid = sid AND db.time BETWEEN start AND end"
static int delete_frags(dbats_handler *dh, DB *db, const char *name,
    int sid, uint32_t start, uint32_t end)
{
    dbats_log(LOG_FINE, "delete_frags %s %d %u..%u", name,
	sid, start, end);
    int rc;
    DBC *dbc;
    rc = db->cursor(db, current_txn(dh), &dbc, 0);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "delete_frags %s %d %u..%u: cursor: %s", name,
	    sid, start, end, db_strerror(rc));
	return rc;
    }
    fragkey_t dbkey = { htonl(sid), htonl(start), htonl(0) };
    DBT_in(dbt_key, &dbkey, sizeof(dbkey));
    dbt_key.ulen = sizeof(dbkey);
    DBT_null(dbt_data);
    // Range cursor search depends on dbkey being {sid, time, frag_id}, in
    // that order, with big endian values.
    rc = dbc->get(dbc, &dbt_key, &dbt_data, DB_SET_RANGE);
    while (1) {
	if (rc == DB_NOTFOUND) break;
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "delete_frags %s %d %u %d: get: %s", name,
		ntohl(dbkey.sid), ntohl(dbkey.time), ntohl(dbkey.frag_id),
		db_strerror(rc));
	    return rc;
	}
	if (ntohl(dbkey.sid) != sid || ntohl(dbkey.time) > end) break;
	rc = dbc->del(dbc, 0);
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "delete_frags %s %d %u %d: del: %s", name,
		ntohl(dbkey.sid), ntohl(dbkey.time), ntohl(dbkey.frag_id),
		db_strerror(rc));
	    return rc;
	} else {
	    dbats_log(LOG_FINE, "delete_frags %d %u %d",
		ntohl(dbkey.sid), ntohl(dbkey.time), ntohl(dbkey.frag_id));
	}
	rc = dbc->get(dbc, &dbt_key, &dbt_data, DB_NEXT);
    }
    return 0;
}

static int read_series_info(dbats_handler *dh, int sid, uint32_t flags)
{
    size_t value_len = sizeof(dbats_series_info);
    char keybuf[32];
    series_info_key(dh, keybuf, sid);

    int rc = raw_db_get(dh, dh->dbMeta, keybuf, strlen(keybuf),
	&dh->series[sid], &value_len, flags);
    if (rc != 0)
	dbats_log(LOG_ERROR, "error getting %s: %s", keybuf, db_strerror(rc));
    return rc;
}

static int write_series_info(dbats_handler *dh, int sid)
{
    char keybuf[32];
    series_info_key(dh, keybuf, sid);
    int rc = raw_db_set(dh, dh->dbMeta, keybuf, strlen(keybuf),
	&dh->series[sid], sizeof(dbats_series_info));
    if (rc != 0)
	dbats_log(LOG_ERROR, "error updating %s: %s", keybuf, db_strerror(rc));
    return rc;
}

int dbats_series_limit(dbats_handler *dh, int sid, int keep)
{
    int rc = 0;

    if (sid < 0 || sid >= dh->cfg.num_series) {
	dbats_log(LOG_ERROR, "bad sid %d", sid);
	return EINVAL;
    }

    rc = begin_transaction(dh, "keep txn");
    if (rc != 0) return rc;

    rc = read_series_info(dh, sid, DB_RMW);
    if (rc != 0) goto abort;

    if (sid == 0) {
	for (int a = 1; a < dh->cfg.num_series; a++) {
	    rc = read_series_info(dh, a, 0);
	    if (rc != 0) goto abort;
	    if (keep > 0 && dh->series[a].steps > keep) {
		dbats_log(LOG_ERROR, "series %d requires keeping at least %d "
		    "primary data points", a, dh->series[a].steps);
		rc = EINVAL;
		goto abort;
	    }
	}
    }

    dbats_series_info *series = &dh->series[sid];
    series->keep = keep;

    if (keep > 0) {
	uint32_t end = series->times.end - keep * series->period;
	if (series->times.start <= end) {
	    rc = delete_frags(dh, dh->dbData, "data", sid, 0, end);
	    if (rc != 0) goto abort;
	    rc = delete_frags(dh, dh->dbIsSet, "is_set", sid, 0, end);
	    if (rc != 0) goto abort;
	    series->times.start = end + series->period;
	}
    }

    rc = write_series_info(dh, sid); // write new values of keep and times.start
    if (rc != 0) goto abort;

    return commit_transaction(dh); // "keep txn"

abort:
    abort_transaction(dh);
    return rc;
}

/*************************************************************************/

// Free contents of in-memory tslice.
static int clear_tslice(dbats_handler *dh, int sid)
{
    dbats_tslice *tslice = dh->tslice[sid];
    dbats_log(LOG_FINE, "Free tslice t=%u sid=%d", tslice->time, sid);

    for (int f = 0; f < tslice->num_frags; f++) {
	if (tslice->frag[f]) {
	    free(tslice->frag[f]);
	    tslice->frag[f] = 0;
	}
	if (tslice->is_set[f]) {
	    free(tslice->is_set[f]);
	    tslice->is_set[f] = 0;
	}
    }

    memset(tslice, 0, sizeof(dbats_tslice));

    return 0;
}

// Writes tslice to db (unless db is readonly).
// Also cleans up in-memory tslice, even if db is readonly.
static int flush_tslice(dbats_handler *dh, int sid)
{
    dbats_log(LOG_FINE, "Flush t=%u sid=%d", dh->tslice[sid]->time, sid);
    size_t frag_size = fragsize(dh);
    char *buf = NULL;
    int rc;
    int changed = 0;

    // Write fragments to the DB
    fragkey_t dbkey = { htonl(sid), htonl(dh->tslice[sid]->time), 0};
    for (int f = 0; f < dh->tslice[sid]->num_frags; f++) {
	if (!dh->tslice[sid]->frag[f]) continue;
	dbkey.frag_id = htonl(f);
	if (dh->cfg.readonly) {
	    // do nothing
	} else if (!dh->tslice[sid]->frag_changed[f]) {
	    dbats_log(LOG_VERYFINE, "Skipping frag %d (unchanged)", f);
	} else {
	    if (dh->cfg.compress) {
		if (!buf) {
		    buf = (char*)emalloc(1 + frag_size + QLZ_OVERHEAD,
			"compression buffer");
		    if (!buf) return errno ? errno : ENOMEM;
		}
		buf[0] = 1; // compression flag
		dh->tslice[sid]->frag[f]->compressed = 1;
		size_t compress_len = qlz_compress(dh->tslice[sid]->frag[f],
		    buf+1, frag_size, dh->state_compress);
		dbats_log(LOG_VERYFINE, "Frag %d compression: %zu -> %zu (%.1f %%)",
		    f, frag_size, compress_len, compress_len*100.0/frag_size);
		rc = raw_db_set(dh, dh->dbData, &dbkey, sizeof(dbkey),
		    buf, compress_len + 1);
		if (rc != 0) return rc;
	    } else {
		dh->tslice[sid]->frag[f]->compressed = 0;
		dbats_log(LOG_VERYFINE, "Frag %d write: %zu", f, frag_size);
		rc = raw_db_set(dh, dh->dbData, &dbkey, sizeof(dbkey),
		    dh->tslice[sid]->frag[f], frag_size);
		if (rc != 0) return rc;
	    }
	    dh->tslice[sid]->frag_changed[f] = 0;
	    changed = 1;

	    rc = raw_db_set(dh, dh->dbIsSet, &dbkey, sizeof(dbkey),
		dh->tslice[sid]->is_set[f], vec_size(ENTRIES_PER_FRAG));
	    if (rc != 0) return rc;
	}
    }
    if (buf) free(buf);

    // We read and write series.times as the last step of a transaction to
    // minimize the time that the lock is held.
    if (changed) {
	// get updated series times from db
	if ((rc = read_series_info(dh, sid, DB_RMW)) != 0)
	    return rc;

	// write new series times to db if needed
	changed = 0;
	if (dh->series[sid].times.start == 0 ||
	    dh->series[sid].times.start > dh->tslice[sid]->time)
	{
	    dh->series[sid].times.start = dh->tslice[sid]->time;
	    changed = 1;
	}
	if (dh->series[sid].times.end < dh->tslice[sid]->time) {
	    dh->series[sid].times.end = dh->tslice[sid]->time;
	    changed = 1;
	}
	if (changed) {
	    if ((rc = write_series_info(dh, sid)) != 0)
		return rc;
	}
    }

    if (!dh->cfg.exclusive || !dh->is_open) {
	clear_tslice(dh, sid);
    }

    return 0;
}

/*************************************************************************/

static void free_isset(dbats_handler *dh)
{
    if (dh->is_set) {
	int tn = (dh->active_end - dh->active_start) / dh->series[0].period + 1;
	for (int ti = 0; ti < tn; ti++) {
	    if (dh->is_set[ti]) {
		for (int f = 0; f < dh->tslice[0]->num_frags; f++) {
		    if (dh->is_set[ti][f]) {
			if (dh->tslice[0]->is_set[f] != dh->is_set[ti][f])
			    free(dh->is_set[ti][f]); // shared; don't free twice
			dh->is_set[ti][f] = 0;
		    }
		}
		free(dh->is_set[ti]);
		dh->is_set[ti] = 0;
	    }
	}
	free(dh->is_set);
	dh->is_set = 0;
    }
}

int dbats_commit(dbats_handler *dh)
{
    if (dh->n_txn <= dh->n_internal_txn) return 0;
    dbats_log(LOG_FINE, "dbats_commit %u", dh->tslice[0]->time);

    free_isset(dh);

    for (int sid = 0; sid < dh->cfg.num_series; sid++) {
	int rc = flush_tslice(dh, sid);
	if (rc != 0) {
	    abort_transaction(dh);
	    return rc;
	}
    }

    return commit_transaction(dh);
}

int dbats_abort(dbats_handler *dh)
{
    if (dh->n_txn <= dh->n_internal_txn) return 0;
    dbats_log(LOG_FINE, "dbats_abort %u", dh->tslice[0]->time);

    free_isset(dh);

    for (int sid = 0; sid < dh->cfg.num_series; sid++) {
	clear_tslice(dh, sid);
    }

    return abort_transaction(dh);
}

/*************************************************************************/

void dbats_close(dbats_handler *dh)
{
    if (!dh->is_open)
	return;
    dh->is_open = 0;

    dbats_commit(dh);

    if (!HAVE_SET_LK_EXCLUSIVE && dh->cfg.exclusive) {
	commit_transaction(dh); // exclusive txn
	dh->n_internal_txn--;
    }

    for (int sid = 0; sid < dh->cfg.num_series; sid++) {
	free(dh->tslice[sid]);
	dh->tslice[sid] = NULL;
    }

    if (dh->db_get_buf) free(dh->db_get_buf);
    if (dh->series) free(dh->series);
    if (dh->tslice) free(dh->tslice);

    //dh->dbMeta->close(dh->dbMeta, 0);
    //dh->dbKeyname->close(dh->dbKeyname, 0);
    //dh->dbKeyid->close(dh->dbKeyid, 0);
    //dh->dbData->close(dh->dbData, 0);
    //dh->dbIsSet->close(dh->dbIsSet, 0);
    dh->dbenv->close(dh->dbenv, 0);

    if (dh->state_decompress)
	free(dh->state_decompress);
    if (dh->state_compress)
	free(dh->state_compress);
    free(dh);
}

/*************************************************************************/

static const time_t time_base = 259200; // 00:00 on first sunday of 1970, UTC

uint32_t dbats_normalize_time(const dbats_handler *dh, int sid, uint32_t *t)
{
    *t -= (*t - time_base) % dh->series[sid].period;
    return *t;
}

/*************************************************************************/

static int load_isset(dbats_handler *dh, fragkey_t *dbkey, uint8_t **dest)
{
    static uint8_t *buf = NULL; // can recycle memory across calls
    int rc;
    size_t value_len = vec_size(ENTRIES_PER_FRAG);

    if (!buf && !(buf = emalloc(vec_size(ENTRIES_PER_FRAG), "is_set")))
	return errno ? errno : ENOMEM; // error

    rc = raw_db_get(dh, dh->dbIsSet, dbkey, sizeof(*dbkey),
	buf, &value_len, !dh->cfg.readonly ? DB_RMW : 0);

    dbats_log(LOG_FINE, "load_isset t=%u, sid=%u, frag_id=%u: "
	"raw_db_get(is_set) = %d",
	ntohl(dbkey->time), ntohl(dbkey->sid), ntohl(dbkey->frag_id), rc);

    if (rc == DB_NOTFOUND) {
	if (ntohl(dbkey->time) == dh->tslice[ntohl(dbkey->sid)]->time) {
	    memset(buf, 0, vec_size(ENTRIES_PER_FRAG));
	    *dest = buf;
	    buf = NULL;
	} else {
	    // return nothing, and keep buf for the next call
	    *dest = NULL;
	}
	return rc; // no match
    } else if (rc != 0) {
	*dest = NULL;
	return rc; // error
    }

    // assert(value_len == vec_size(ENTRIES_PER_FRAG));
    *dest = buf;
    buf = NULL;

    if (ntohl(dbkey->sid) == 0 && dh->active_last_data < ntohl(dbkey->time))
	dh->active_last_data = ntohl(dbkey->time);

    return 0; // ok
}

static int load_frag(dbats_handler *dh, uint32_t t, int sid,
    uint32_t frag_id)
{
    // load fragment
    fragkey_t dbkey = { htonl(sid), htonl(t), htonl(frag_id) };
    int rc;

    assert(!dh->tslice[sid]->is_set[frag_id]);
    rc = load_isset(dh, &dbkey, &dh->tslice[sid]->is_set[frag_id]);
    if (rc == DB_NOTFOUND) return 0; // no match
    if (rc != 0) return rc; // error

    if (dh->db_get_buf_len < fragsize(dh) + QLZ_OVERHEAD) {
	if (dh->db_get_buf) free(dh->db_get_buf);
	dh->db_get_buf_len = fragsize(dh) + QLZ_OVERHEAD;
	dh->db_get_buf = emalloc(dh->db_get_buf_len, "get buffer");
	if (!dh->db_get_buf) return errno ? errno : ENOMEM;
    }

    size_t value_len = dh->db_get_buf_len;
    rc = raw_db_get(dh, dh->dbData, &dbkey, sizeof(dbkey),
	dh->db_get_buf, &value_len, !dh->cfg.readonly ? DB_RMW : 0);
    dbats_log(LOG_FINE, "load_frag t=%u, sid=%u, frag_id=%u: "
	"raw_db_get(frag) = %d", t, sid, frag_id, rc);
    if (rc != 0) {
	if (rc == DB_NOTFOUND) return 0; // no match
	return rc; // error
    }

    int compressed = ((dbats_frag*)dh->db_get_buf)->compressed;
    size_t len = !compressed ? fragsize(dh) :
	qlz_size_decompressed((void*)(dh->db_get_buf+1));
    void *ptr = malloc(len);
    if (!ptr) {
	rc = errno;
	dbats_log(LOG_ERROR, "Can't allocate %zu bytes for frag "
	    "t=%u, sid=%u, frag_id=%u", len, t, sid, frag_id);
	return rc; // error
    }

    if (compressed) {
	// decompress fragment
	if (!dh->state_decompress) {
	    dh->state_decompress = ecalloc(1, sizeof(qlz_state_decompress),
		"qlz_state_decompress");
	    if (!dh->state_decompress) return errno ? errno : ENOMEM;
	}
	len = qlz_decompress((void*)(dh->db_get_buf+1), ptr, dh->state_decompress);
	// assert(len == fragsize(dh));
	dbats_log(LOG_VERYFINE, "decompressed frag t=%u, sid=%u, frag_id=%u: "
	    "%u -> %u (%.1f%%)",
	    t, sid, frag_id, value_len, len, len*100.0/value_len);

    } else {
	// copy fragment
	// XXX TODO: don't memcpy; get directly into ptr
	memcpy(ptr, dh->db_get_buf, len);
	dbats_log(LOG_VERYFINE, "copied frag t=%u, sid=%u, frag_id=%u",
	    t, sid, frag_id);
    }

    dh->tslice[sid]->frag[frag_id] = ptr;

    dbats_tslice *tslice = dh->tslice[sid];
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;

    return 0;
}

// Instantiate dh->is_set[*][frag_id]
static int instantiate_isset_frags(dbats_handler *dh, int fid)
{
    if (dh->cfg.readonly)
	return 0; // dh->is_set is never needed in readonly mode

    int tn = (dh->active_end - dh->active_start) / dh->series[0].period + 1;

    if (!dh->is_set) {
	dh->is_set = ecalloc(tn, sizeof(uint8_t**), "dh->is_set");
	if (!dh->is_set) return errno ? errno : ENOMEM;
    }

    uint32_t t = dh->active_start;
    for (int ti = 0; ti < tn; ti++, t += dh->series[0].period) {
	fragkey_t dbkey = { htonl(0), htonl(t), 0 };
	if (!dh->is_set[ti]) {
	    dh->is_set[ti] = ecalloc(MAX_NUM_FRAGS, sizeof(uint8_t*),
		"dh->is_set[ti]");
	    if (!dh->is_set[ti]) return errno ? errno : ENOMEM;
	}

	if (t == dh->tslice[0]->time && dh->tslice[0]->is_set[fid]) {
	    assert(!dh->is_set[ti][fid]);
	    dh->is_set[ti][fid] = dh->tslice[0]->is_set[fid]; // share
	} else if (!dh->is_set[ti][fid]) {
	    dbkey.frag_id = htonl(fid);
	    assert(!dh->is_set[ti][fid]);
	    int rc = load_isset(dh, &dbkey, &dh->is_set[ti][fid]);
	    if (rc != 0 && rc != DB_NOTFOUND)
		return rc; // error
	    if (t == dh->tslice[0]->time) {
		assert(!dh->tslice[0]->is_set[fid]);
		dh->tslice[0]->is_set[fid] = dh->is_set[ti][fid]; // share
	    }
	}
    }
    return 0;
}

int dbats_num_keys(dbats_handler *dh, uint32_t *num_keys)
{
    int rc;
    DB_BTREE_STAT *stats;

    rc = dh->dbKeyid->stat(dh->dbKeyid, current_txn(dh), &stats, DB_FAST_STAT);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error getting keys stats: %s", db_strerror(rc));
	return rc;
    }

    *num_keys = stats->bt_nkeys;
    free(stats);
    return 0;
}

int dbats_select_time(dbats_handler *dh, uint32_t time_value, uint32_t flags)
{
    int rc;

    dbats_log(LOG_FINE, "select_time %u", time_value);

    if ((rc = dbats_commit(dh)) != 0)
	return rc;

    if ((rc = begin_transaction(dh, "tslice txn")) != 0)
	return rc;

    dh->preload = !!(flags & DBATS_PRELOAD);

    uint32_t min_time = 0;
    uint32_t max_time = 0;

    if (dh->serialize) {
	// Lock "series0" to enforce one writer at a time.
	// Timeslice transactions are sufficiently complex that they end up
	// being serialized anyway.  By serializing up front, we avoid all
	// deadlocks and don't need sub-transactions around dbats_get() and
	// dbats_set().
	int timeout = 10;
	const char *keybuf = "series0";
	while (1) {
	    size_t value_len;
	    value_len = sizeof(dbats_series_info);
	    int dbflags = dh->cfg.readonly ? 0 : DB_RMW;
	    rc = raw_db_get(dh, dh->dbMeta, keybuf, strlen(keybuf),
		&dh->series[0], &value_len, dbflags);
	    if (rc == 0) break;
	    if (rc != DB_LOCK_DEADLOCK || --timeout <= 0) {
		dbats_log(LOG_ERROR, "error getting %s: %s",
		    keybuf, db_strerror(rc));
		return rc;
	    }
	    dbats_log(LOG_FINE, "select_time %u: get %s: deadlock, timeout=%d",
		time_value, keybuf, timeout);
	}
	dbats_log(LOG_FINE, "select_time %u: got %s", time_value, keybuf);
    }

    for (int sid = 0; sid < dh->cfg.num_series; sid++) {
	dbats_tslice *tslice = dh->tslice[sid];
	uint32_t t = time_value;

	dbats_normalize_time(dh, sid, &t);

	if (dh->cfg.exclusive) {
	    if (tslice->time == t) {
		dbats_log(LOG_FINE, "select_time %u, sid=%d: already loaded",
		    t, sid);
		continue;
	    }
	    // free obsolete fragments
	    clear_tslice(dh, sid);
	}

	tslice->time = t;
	if (sid == 0)
	    dh->active_last_data = t;

	if (!min_time || t < min_time)
	    min_time = t;
	t += dh->series[sid].period - dh->series[0].period;
	if (t > max_time)
	    max_time = t;
    }
    dh->active_start = min_time;
    dh->active_end = max_time;

    if (!dh->preload)
	return 0;

    uint32_t num_keys;
    if ((rc = dbats_num_keys(dh, &num_keys)) != 0)
	return rc;

    for (int sid = 0; sid < dh->cfg.num_series; sid++) {
	dbats_tslice *tslice = dh->tslice[sid];
	uint32_t t = tslice->time;
	int loaded = 0;

	tslice->num_frags = div_ceil(num_keys, ENTRIES_PER_FRAG);
	for (uint32_t frag_id = 0; frag_id < tslice->num_frags; frag_id++) {
	    if ((rc = load_frag(dh, t, sid, frag_id)) != 0)
		return rc;
	    if (tslice->frag[frag_id])
		loaded++;
	}
	dbats_log(LOG_FINE, "select_time %u: sid=%d, loaded %u/%u fragments",
	    time_value, sid, loaded, tslice->num_frags);
    }

    for (int frag_id = 0; frag_id < dh->tslice[0]->num_frags; frag_id++) {
	instantiate_isset_frags(dh, frag_id);
    }

    return 0;
}

/*************************************************************************/

int dbats_get_key_id(dbats_handler *dh, const char *key,
    uint32_t *key_id_p, uint32_t flags)
{
    size_t keylen = strlen(key);
    size_t len = sizeof(*key_id_p);
    int rc;

    rc = raw_db_get(dh, dh->dbKeyname, key, keylen, key_id_p, &len,
	DB_READ_COMMITTED);
    if (rc == 0) {
	// found it
	dbats_log(LOG_FINEST, "Found key #%u: %s", *key_id_p, key);
	return 0;

    } else if (rc != DB_NOTFOUND) {
	// error
	return rc;

    } else if (!(flags & DBATS_CREATE)) {
	// didn't find, and can't create
	dbats_log(LOG_FINE, "Key not found: %s", key);
	return rc;

    } else {
	// create
	db_recno_t recno;
	DBT_in(dbt_keyname, (void*)key, strlen(key));
	DBT_out(dbt_keyrecno, &recno, sizeof(recno)); // put() will fill recno
	rc = dh->dbKeyid->put(dh->dbKeyid, current_txn(dh), &dbt_keyrecno,
	    &dbt_keyname, DB_APPEND);
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "Error creating keyid for %s: %s",
		key, db_strerror(rc));
	    return rc;
	}
	*key_id_p = recno - 1;
	if (*key_id_p >= MAX_NUM_FRAGS * ENTRIES_PER_FRAG) {
	    dbats_log(LOG_ERROR, "Out of space for key %s", key);
	    rc = dh->dbKeyid->del(dh->dbKeyid, current_txn(dh), &dbt_keyrecno, 0);
	    return ENOMEM;
	}

	DBT_in(dbt_keyid, key_id_p, sizeof(*key_id_p));
	rc = dh->dbKeyname->put(dh->dbKeyname, current_txn(dh), &dbt_keyname,
	    &dbt_keyid, DB_NOOVERWRITE);
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "Error creating keyname %s -> %u: %s",
		key, *key_id_p, db_strerror(rc));
	    return rc;
	}
	dbats_log(LOG_FINEST, "Assigned key #%u: %s", *key_id_p, key);

	return 0;
    }
}

int dbats_get_key_name(dbats_handler *dh, uint32_t key_id, char *namebuf)
{
    int rc;
    db_recno_t recno = key_id + 1;
    uint32_t value_len = DBATS_KEYLEN - 1;

    rc = raw_db_get(dh, dh->dbKeyid, &recno, sizeof(recno),
	namebuf, &value_len, DB_READ_COMMITTED);
    if (rc != 0) return rc;
    namebuf[value_len] = '\0';
    return 0;
}

/*************************************************************************/

static int instantiate_frag_func(dbats_handler *dh, int sid, uint32_t frag_id)
{
    int rc;
    dbats_tslice *tslice = dh->tslice[sid];

    if (!dh->preload || dh->cfg.readonly) {
	// Try to load the fragment.
	rc = load_frag(dh, tslice->time, sid, frag_id);
	if (rc != 0) // error
	    return rc;
	if (tslice->frag[frag_id]) { // found it
	    if (sid == 0)
		rc = instantiate_isset_frags(dh, frag_id);
	    return rc;
	}
	if (dh->cfg.readonly) // didn't find it and can't create it
	    return DB_NOTFOUND; // fragment not found
    }

    // Allocate a new fragment.
    dbats_log(LOG_VERYFINE, "grow tslice for time %u, sid %d, frag_id %u",
	tslice->time, sid, frag_id);

    size_t len = fragsize(dh);
    if (!(tslice->frag[frag_id] = ecalloc(1, len, "tslice->frag[frag_id]"))) {
	return errno ? errno : ENOMEM;
    }

    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;
    if (sid == 0) {
	rc = instantiate_isset_frags(dh, frag_id);
	if (rc != 0) return rc;
    }

    dbats_log(LOG_VERYFINE, "Grew tslice to %u elements",
	tslice->num_frags * ENTRIES_PER_FRAG);

    return 0;
}

// inline version handles the common case without a function call
static inline int instantiate_frag(dbats_handler *dh, int sid, uint32_t frag_id)
{
    return dh->tslice[sid]->frag[frag_id] ?
	0 : // We already have the fragment.
	instantiate_frag_func(dh, sid, frag_id);
}

/*************************************************************************/

int dbats_set(dbats_handler *dh, uint32_t key_id, const dbats_value *valuep)
{
    dbats_log(LOG_VERYFINE, "dbats_set %u #%u = %" PRIval,
	dh->tslice[0]->time, key_id, valuep[0]); // XXX
    int rc;

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

    if (!dh->is_open || dh->cfg.readonly) {
	dbats_log(LOG_ERROR, "is_open=%d, readonly=%d",
	    dh->is_open, dh->cfg.readonly);
	return EPERM;
    }

    if (dh->n_txn <= dh->n_internal_txn) {
	dbats_log(LOG_ERROR, "dbats_set: no transaction");
	return -1;
    }

retry:
    if (!dh->serialize)
	begin_transaction(dh, "set txn");

    if ((rc = instantiate_frag(dh, 0, frag_id)) != 0) {
	if (!dh->serialize) {
	    abort_transaction(dh); // set txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }

    uint8_t was_set = vec_test(dh->tslice[0]->is_set[frag_id], offset);
    if (was_set & !dh->cfg.updatable) {
	dbats_log(LOG_ERROR, "value is already set: t=%" PRIu32 " keyid=%d",
	    dh->tslice[0]->time, key_id);
	return EEXIST;
    }
    dbats_value *oldvaluep = valueptr(dh, 0, frag_id, offset);

    // For each sid, aggregate *valuep into tslice[sid].
    for (int sid = 1; sid < dh->cfg.num_series; sid++) {
	if ((rc = instantiate_frag(dh, sid, frag_id)) != 0) {
	    if (!dh->serialize) {
		abort_transaction(dh); // set txn
		if (rc == DB_LOCK_DEADLOCK) goto retry;
	    }
	    return rc;
	}

	uint8_t changed = 0;
	uint8_t failed = 0;
	dbats_value *aggval = valueptr(dh, sid, frag_id, offset);
	uint32_t aggstart = dh->tslice[sid]->time;
	uint32_t aggend = min(dh->active_last_data,
	    aggstart + dh->series[sid].period - dh->series[0].period);

	// Count the number of steps contributing to the aggregate.
	int n = 0;
	{
	    int ti = (aggstart - dh->active_start) / dh->series[0].period;
	    uint32_t t = aggstart;
	    while (t <= aggend) {
		if (t == dh->tslice[0]->time ||
		    vec_test(dh->is_set[ti][frag_id], offset))
			n++;
		ti++;
		t += dh->series[0].period;
	    }
	}

	dbats_log(LOG_FINEST, "agg %d: aggval=%" PRIval " n=%d",
	    sid, aggval[0], n); // XXX

	for (int i = 0; i < dh->cfg.values_per_entry; i++) {
	    if (was_set && valuep[i] == oldvaluep[i]) {
		continue; // value did not change; no need to change agg value
	    }
	    switch (dh->series[sid].func) {
	    case DBATS_AGG_MIN:
		if (n == 1 || valuep[i] <= aggval[i]) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else if (was_set && oldvaluep[i] == aggval[i]) {
		    // XXX TODO: Find the min value among all the steps.
		    failed = ENOSYS; // XXX
		}
		break;
	    case DBATS_AGG_MAX:
		if (n == 1 || valuep[i] >= aggval[i]) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else if (was_set && oldvaluep[i] == aggval[i]) {
		    // XXX TODO: Find the max value among all the steps.
		    failed = ENOSYS; // XXX
		}
		break;
	    case DBATS_AGG_AVG:
		{
		    double *daggval = ((double*)&aggval[i]);
		    if (n == 1) {
			*daggval = valuep[i];
			changed = 1;
		    } else {
			double old_daggval = *daggval;
			if (was_set)
			    *daggval -= (oldvaluep[i] - *daggval) / n;
			*daggval += (valuep[i] - *daggval) / n;
			changed = (*daggval != old_daggval);
		    }
		}
		break;
	    case DBATS_AGG_LAST:
		if (dh->tslice[0]->time >= dh->active_last_data) {
		    // common case: value is latest ever seen
		    aggval[i] = valuep[i];
		    changed = 1;
		} else {
		    // find time of last sample for this key within agg period
		    uint32_t t = aggend;
		    int ti = (t - dh->active_start) / dh->series[0].period;
		    while (t >= aggstart) {
			if (t == dh->tslice[0]->time) {
			    // the new value is the last
			    aggval[i] = valuep[i];
			    changed = 1;
			    break;
			}
			if (vec_test(dh->is_set[ti][frag_id], offset))
			    // an existing value is later than the new value
			    break;
			t -= dh->series[0].period;
			ti--;
		    }
		}
		break;
	    case DBATS_AGG_SUM:
		if (n == 1) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else if (was_set || valuep[i] != 0) {
		    if (was_set)
			aggval[i] -= oldvaluep[i];
		    aggval[i] += valuep[i];
		    changed = 1;
		}
		break;
	    }
	}

	if (changed || failed) {
	    int level = failed ? LOG_ERROR : LOG_VERYFINE;
	    if (level <= dbats_log_level) { 
		char aggbuf[64];
		if (dh->series[sid].func == DBATS_AGG_AVG)
		    sprintf(aggbuf, "%f", ((double*)aggval)[0]);
		else
		    sprintf(aggbuf, "%" PRIval, aggval[0]);
		dbats_log(level, "%s set value sid=%d frag_id=%u "
		    "offset=%" PRIu32 " value_len=%u aggval=%s",
		    failed ? "Failed to" : "Successfully",
		    sid, frag_id, offset, dh->cfg.entry_size, aggbuf);
	    }
	}
	if (changed) {
	    // XXX if (n >= xff * steps)
	    vec_set(dh->tslice[sid]->is_set[frag_id], offset);
	    dh->tslice[sid]->frag_changed[frag_id] = 1;
	} else if (failed) {
	    if (!dh->serialize)
		abort_transaction(dh); // set txn
	    return failed;
	}
    }

    // Set value at sid 0 (after aggregations because aggregations need
    // both old and new values)
    memcpy(oldvaluep, valuep, dh->cfg.entry_size);
    vec_set(dh->tslice[0]->is_set[frag_id], offset);
    dh->tslice[0]->frag_changed[frag_id] = 1;
    dbats_log(LOG_VERYFINE, "Succesfully set value "
	"sid=%d frag_id=%u offset=%" PRIu32 " value_len=%u value=%" PRIval,
	0, frag_id, offset, dh->cfg.entry_size, valuep[0]);

    if (!dh->serialize)
	commit_transaction(dh); // set txn
    return 0;
}

int dbats_set_by_key(dbats_handler *dh, const char *key,
    const dbats_value *valuep, int flags)
{
    int rc;
    uint32_t key_id;

    if ((rc = dbats_get_key_id(dh, key, &key_id, flags)) != 0)
	return rc;
    return dbats_set(dh, key_id, valuep);
}

/*************************************************************************/

int dbats_get(dbats_handler *dh, uint32_t key_id,
    const dbats_value **valuepp, int sid)
{
    int rc;
    dbats_tslice *tslice = dh->tslice[sid];

    if (!dh->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if (dh->n_txn <= dh->n_internal_txn) {
	dbats_log(LOG_ERROR, "dbats_get: no transaction");
	return -1;
    }

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

retry:
    if (!dh->serialize)
	begin_transaction(dh, "get txn");

    if ((rc = instantiate_frag(dh, sid, frag_id)) == 0) {
	if (!vec_test(tslice->is_set[frag_id], offset)) {
	    char keyname[DBATS_KEYLEN] = "";
	    dbats_get_key_name(dh, key_id, keyname);
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, sid, keyname);
	    *valuepp = NULL;
	    if (!dh->serialize)
		commit_transaction(dh); // get txn
	    return DB_NOTFOUND;
	}
	if (dh->series[sid].func == DBATS_AGG_AVG) {
	    double *dval = (double*)valueptr(dh, sid, frag_id, offset);
	    static dbats_value *avg_buf = NULL;
	    if (!avg_buf) {
		avg_buf = emalloc(dh->cfg.entry_size * dh->cfg.values_per_entry,
		    "avg_buf");
		if (!avg_buf) {
		    if (!dh->serialize)
			abort_transaction(dh); // get txn
		    return errno ? errno : ENOMEM;
		}
	    }
	    for (int i = 0; i < dh->cfg.values_per_entry; i++)
		avg_buf[i] = dval[i] + 0.5;
	    *valuepp = avg_buf;
	} else {
	    *valuepp = valueptr(dh, sid, frag_id, offset);
	}
	dbats_log(LOG_VERYFINE, "Succesfully read value off=%" PRIu32 " len=%u",
	    offset, dh->cfg.entry_size);
	if (!dh->serialize)
	    commit_transaction(dh); // get txn
	return 0;

    } else if (rc == DB_NOTFOUND) {
	char keyname[DBATS_KEYLEN] = "";
	dbats_get_key_name(dh, key_id, keyname);
	dbats_log(LOG_WARNING, "Value unset (f): %u %d %s",
	    tslice->time, sid, keyname);
	*valuepp = NULL;
	if (!dh->serialize)
	    commit_transaction(dh); // get txn
	return rc;

    } else {
	if (!dh->serialize) {
	    abort_transaction(dh); // get txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }
}

int dbats_get_double(dbats_handler *dh, uint32_t key_id,
    const double **valuepp, int sid)
{
    int rc;
    dbats_tslice *tslice = dh->tslice[sid];

    if (!dh->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if (dh->series[sid].func != DBATS_AGG_AVG) {
	dbats_log(LOG_ERROR, "Aggregation %d is not a double", sid);
	*valuepp = NULL;
	return -1;
    }

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

retry:
    if (!dh->serialize)
	begin_transaction(dh, "get txn");

    if ((rc = instantiate_frag(dh, sid, frag_id)) == 0) {
	if (!vec_test(tslice->is_set[frag_id], offset)) {
	    char keyname[DBATS_KEYLEN] = "";
	    dbats_get_key_name(dh, key_id, keyname);
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, sid, keyname);
	    *valuepp = NULL;
	    if (!dh->serialize)
		commit_transaction(dh); // get txn
	    return DB_NOTFOUND;
	}
	*valuepp = (double*)valueptr(dh, sid, frag_id, offset);
	dbats_log(LOG_VERYFINE, "Succesfully read value off=%" PRIu32 " len=%u",
	    offset, dh->cfg.entry_size);
	if (!dh->serialize)
	    commit_transaction(dh); // get txn
	return 0;

    } else if (rc == DB_NOTFOUND) {
	char keyname[DBATS_KEYLEN] = "";
	dbats_get_key_name(dh, key_id, keyname);
	dbats_log(LOG_WARNING, "Value unset (f): %u %d %s",
	    tslice->time, sid, keyname);
	*valuepp = NULL;
	if (!dh->serialize)
	    commit_transaction(dh); // get txn
	return rc;

    } else {
	if (!dh->serialize) {
	    abort_transaction(dh); // get txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }
}

int dbats_get_by_key(dbats_handler *dh, const char *key,
    const dbats_value **valuepp, int sid)
{
    int rc;
    uint32_t key_id;

    if ((rc = dbats_get_key_id(dh, key, &key_id, 0)) != 0) {
	return rc;
    }
    return dbats_get(dh, key_id, valuepp, sid);
}

/*************************************************************************/

int dbats_walk_keyname_start(dbats_handler *dh)
{
    int rc;

    rc = dh->dbKeyname->cursor(dh->dbKeyname, NULL, &dh->keyname_cursor, 0);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyname_start: %s",
	    db_strerror(rc));
	return -1;
    }
    return 0;
}

int dbats_walk_keyname_next(dbats_handler *dh, uint32_t *key_id_p,
    char *namebuf)
{
    int rc;
    DBT_out(dbt_keyid, key_id_p, sizeof(*key_id_p));
    DBT_out(dbt_keyname, namebuf, DBATS_KEYLEN-1);
    rc = dh->keyname_cursor->get(dh->keyname_cursor,
	&dbt_keyname, &dbt_keyid, DB_NEXT | DB_READ_COMMITTED);
    if (rc != 0) {
	if (rc != DB_NOTFOUND)
	    dbats_log(LOG_ERROR, "Error in dbats_walk_keyname_next: %s",
		db_strerror(rc));
	return -1;
    }
    if (namebuf)
	namebuf[dbt_keyname.size] = '\0';
    return 0;
}

int dbats_walk_keyname_end(dbats_handler *dh)
{
    int rc;

    if ((rc = dh->keyname_cursor->close(dh->keyname_cursor)) != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyname_end: %s",
	    db_strerror(rc));
	return -1;
    }
    return 0;
}

/*************************************************************************/

int dbats_walk_keyid_start(dbats_handler *dh)
{
    int rc = dh->dbKeyid->cursor(dh->dbKeyid, NULL, &dh->keyid_cursor, 0);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyid_start: %s",
	    db_strerror(rc));
	return -1;
    }
    return 0;
}

int dbats_walk_keyid_next(dbats_handler *dh, uint32_t *key_id_p,
    char *namebuf)
{
    int rc;
    db_recno_t recno;

    DBT_out(dbt_keyrecno, &recno, sizeof(recno));
    DBT_out(dbt_keyname, namebuf, DBATS_KEYLEN-1);
    rc = dh->keyid_cursor->get(dh->keyid_cursor,
	&dbt_keyrecno, &dbt_keyname, DB_NEXT | DB_READ_COMMITTED);
    if (rc != 0) {
	if (rc != DB_NOTFOUND)
	    dbats_log(LOG_ERROR, "Error in dbats_walk_keyid_next: %s",
		db_strerror(rc));
	return -1;
    }
    *key_id_p = recno - 1;
    if (namebuf)
	namebuf[dbt_keyname.size] = '\0';
    return 0;
}

int dbats_walk_keyid_end(dbats_handler *dh)
{
    int rc = dh->keyid_cursor->close(dh->keyid_cursor);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyid_end: %s",
	    db_strerror(rc));
	return -1;
    }
    return 0;
}

/*************************************************************************/

const dbats_config *dbats_get_config(dbats_handler *dh)
    { return &dh->cfg; }

const dbats_series_info *dbats_get_series_info(dbats_handler *dh, int sid)
    { return &dh->series[sid]; }

/*************************************************************************/

void dbats_stat_print(const dbats_handler *dh) {
    int rc;
    
    if ((rc = dh->dbMeta->stat_print(dh->dbMeta, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping Meta stats: %s", db_strerror(rc));
    if ((rc = dh->dbKeyname->stat_print(dh->dbKeyname, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping Keyname stats: %s", db_strerror(rc));
    if ((rc = dh->dbKeyid->stat_print(dh->dbKeyid, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping Keyid stats: %s", db_strerror(rc));
    if ((rc = dh->dbData->stat_print(dh->dbData, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping Data stats: %s", db_strerror(rc));
    if ((rc = dh->dbIsSet->stat_print(dh->dbIsSet, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping IsSet stats: %s", db_strerror(rc));
}

