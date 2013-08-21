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

#include "dbats.h"
#include "quicklz.h"

// Limits
#define ENTRIES_PER_FRAG    10000 // number of entries in a fragment
#define MAX_NUM_FRAGS       16384 // max number of fragments in a tslice
#define MAX_NUM_AGGS           16 // max number of aggregations

#define HAVE_SET_LK_EXCLUSIVE 0

/*************************************************************************/
// compile-time assertion (works anywhere a declaration is allowed)
#define ct_assert(expr, label)  enum { ASSERTION_ERROR__##label = 1/(!!(expr)) };

ct_assert((sizeof(double) <= sizeof(dbats_value)), dbats_value_is_smaller_than_double)

/*************************************************************************/

// bit vector
#define vec_size(N)        (((N)+7)/8)                    // size for N bits
#define vec_set(vec, i)    (vec[(i)/8] |= (1<<((i)%8)))   // set the ith bit
#define vec_reset(vec, i)  (vec[(i)/8] &= ~(1<<((i)%8)))  // reset the ith bit
#define vec_test(vec, i)   (vec && (vec[(i)/8] & (1<<((i)%8))))    // test the ith bit

#define valueptr(handler, agg_id, frag_id, offset) \
    ((dbats_value*)(&handler->tslice[agg_id]->frag[frag_id]->data[offset * handler->cfg.entry_size]))

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

#define fragsize(handler) (sizeof(dbats_frag) + ENTRIES_PER_FRAG * (handler)->cfg.entry_size)

typedef struct {
    uint32_t time;
    int agg_id;
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
    DB *dbData;                // {time, agg_id, frag_id} -> value fragment
    DB *dbIsSet;               // {time, agg_id, frag_id} -> is_set fragment
    DBC *keyname_cursor;       // for iterating over key names
    DBC *keyid_cursor;         // for iterating over key ids
    dbats_tslice **tslice;     // a tslice for each aggregation level
    dbats_agg *agg;            // parameters for each aggregation level
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

#define current_txn(handler) ((handler)->txn[(handler)->n_txn])

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

static inline uint32_t min(uint32_t a, uint32_t b) { return a < b ? a : b; }
static inline uint32_t max(uint32_t a, uint32_t b) { return a > b ? a : b; }

#if 1
static void *emalloc(size_t size, const char *msg)
{
    void *ptr = malloc(size);
    if (!ptr) {
	int err = errno;
	dbats_log(LOG_WARNING, "Can't allocate %zu bytes for %s", size, msg);
	errno = err;
    }
    return ptr;
}

static void *ecalloc(size_t n, size_t size, const char *msg)
{
    void *ptr = calloc(n, size);
    if (!ptr) {
	int err = errno;
	dbats_log(LOG_WARNING, "Can't allocate %zu * %zu bytes for %s", n, size, msg);
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

static inline int begin_transaction(dbats_handler *handler, const char *name)
{
    if (handler->cfg.no_txn) return 0;
    dbats_log(LOG_FINE, "begin transaction: %s", name);
    int rc;
    DB_TXN *parent = current_txn(handler);
    handler->n_txn++;
    if (handler->n_txn > N_TXN) {
	dbats_log(LOG_ERROR, "begin transaction: %s: too many transactions", name);
	exit(-1);
    }
    DB_TXN **childp = &current_txn(handler);
    if ((rc = handler->dbenv->txn_begin(handler->dbenv, parent, childp, DB_TXN_BULK)) != 0) {
	dbats_log(LOG_ERROR, "begin transaction: %s: %s", name, db_strerror(rc));
	handler->n_txn--;
	return rc;
    }
    (*childp)->set_name(*childp, name);
    return 0;
}

static inline int commit_transaction(dbats_handler *handler)
{
    if (!handler->n_txn) return 0;
    int rc;
    DB_TXN *txn = current_txn(handler);
    handler->n_txn--;
    const char *name;
    txn->get_name(txn, &name);
    dbats_log(LOG_FINE, "commit transaction: %s", name);
    if ((rc = txn->commit(txn, 0)) != 0)
	dbats_log(LOG_ERROR, "commit transaction: %s: %s", name, db_strerror(rc));
    rc = handler->dbenv->txn_checkpoint(handler->dbenv, 256, 0, 0);
    if (rc != 0)
	dbats_log(LOG_ERROR, "txn_checkpoint: %s", db_strerror(rc));
    return rc;
}

static int abort_transaction(dbats_handler *handler)
{
    if (!handler->n_txn) return 0;
    int rc;
    DB_TXN *txn = current_txn(handler);
    handler->n_txn--;
    const char *name;
    txn->get_name(txn, &name);
    dbats_log(LOG_FINE, "abort transaction: %s", name);
    if ((rc = txn->abort(txn)) != 0)
	dbats_log(LOG_ERROR, "abort transaction: %s: %s", name, db_strerror(rc));
    return rc;
}

static int raw_db_open(dbats_handler *handler, DB **dbp, const char *dbname,
    DBTYPE dbtype, uint32_t flags, int mode)
{
    int rc;

    if ((rc = db_create(dbp, handler->dbenv, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error creating DB handler %s/%s: %s",
	    handler->path, dbname, db_strerror(rc));
	return rc;
    }

    if (handler->cfg.exclusive) {
#if HAVE_SET_LK_EXCLUSIVE
	if ((rc = (*dbp)->set_lk_exclusive(*dbp, 1)) != 0) {
	    dbats_log(LOG_ERROR, "Error obtaining exclusive lock on %s/%s: %s",
		handler->path, dbname, db_strerror(rc));
	    return rc;
	}
#endif
    }

    uint32_t dbflags = 0;
    if (!handler->cfg.exclusive)
	dbflags |= DB_THREAD;
    if (flags & DBATS_CREATE)
	dbflags |= DB_CREATE;
    if (flags & DBATS_READONLY)
	dbflags |= DB_RDONLY;

    rc = (*dbp)->open(*dbp, current_txn(handler), dbname, NULL, dbtype, dbflags, mode);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s/%s (mode=%o): %s",
	    handler->path, dbname, mode, db_strerror(rc));
	(*dbp)->close(*dbp, 0);
	return rc;
    }
    return 0;
}

static int raw_db_set(dbats_handler *handler, DB *db,
    const void *key, uint32_t key_len,
    const void *value, uint32_t value_len)
{
    int rc;

    if (db == handler->dbData) {
	fragkey_t *fragkey = (fragkey_t*)key;
	dbats_log(LOG_FINEST, "raw_db_set Data: t=%u agg=%d frag=%u, compress=%d, len=%u",
	    fragkey->time, fragkey->agg_id, fragkey->frag_id, *(uint8_t*)value, value_len);
    }

    if (handler->cfg.readonly) {
	const char *dbname;
	db->get_dbname(db, &dbname, NULL);
	dbats_log(LOG_WARNING, "Unable to set value in database \"%s\" "
	    "(read-only mode)", dbname);
	return EPERM;
    }

    DBT_in(dbt_key, (void*)key, key_len);
    DBT_in(dbt_data, (void*)value, value_len);

    if ((rc = db->put(db, current_txn(handler), &dbt_key, &dbt_data, 0)) != 0) {
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
static int raw_db_get(dbats_handler *handler, DB* db,
    const void *key, uint32_t key_len, void *value, size_t *value_len,
    uint32_t dbflags)
{
    int rc;

    DBT_in(dbt_key, (void*)key, key_len);
    DBT_out(dbt_data, value, *value_len);
    if ((rc = db->get(db, current_txn(handler), &dbt_key, &dbt_data, dbflags)) == 0) {
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
static void raw_db_delete(const dbats_handler *handler, DB *db,
    const void *key, uint32_t key_len)
{
    if (handler->cfg.readonly) {
	dbats_log(LOG_WARNING, "Unable to delete value (read-only mode)");
	return EPERM;
    }

    DBT_in(dbt_key, (void*)key, key_len);
    if (db->del(db, NULL, &dbt_key, 0) != 0)
	dbats_log(LOG_WARNING, "Error while deleting key");
}
*/

/*************************************************************************/

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
    int dbmode = 00666;
    uint32_t dbflags = DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG |
	DB_INIT_TXN | DB_THREAD | DB_REGISTER | DB_RECOVER | DB_CREATE;
    int is_new = 0;

    dbats_handler *handler = ecalloc(1, sizeof(dbats_handler), "handler");
    if (!handler) return NULL;
    handler->cfg.readonly = !!(flags & DBATS_READONLY);
    handler->cfg.compress = !(flags & DBATS_UNCOMPRESSED);
    handler->cfg.exclusive = !!(flags & DBATS_EXCLUSIVE);
    handler->cfg.no_txn = !!(flags & DBATS_NO_TXN);
    handler->cfg.values_per_entry = 1;
    handler->serialize = 1;
    handler->path = path;

    if ((rc = db_env_create(&handler->dbenv, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error creating DB env: %s",
	    db_strerror(rc));
	return NULL;
    }

    handler->dbenv->set_errpfx(handler->dbenv, path);
    handler->dbenv->set_errfile(handler->dbenv, stderr);

    handler->dbenv->set_verbose(handler->dbenv, DB_VERB_FILEOPS | DB_VERB_RECOVERY | DB_VERB_REGISTER, 1);
    handler->dbenv->set_msgfile(handler->dbenv, stderr);

    if (flags & DBATS_CREATE) {
	if (handler->cfg.readonly) {
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
		    dbats_log(LOG_ERROR, "Error checking %s: %s", filename, strerror(errno));
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
	    dbats_log(LOG_ERROR, "Error opening %s: %s", filename, strerror(errno));
	    return NULL;
	}

	fprintf(file, "log_set_config DB_LOG_AUTO_REMOVE on\n");

	// XXX These should be based on max expected number of metric keys?
	// 40000 is enough to insert at least 2000000 metric keys.
	fprintf(file, "set_lk_max_locks 40000\n");
	fprintf(file, "set_lk_max_objects 40000\n");

	fclose(file);
    }

    if ((rc = handler->dbenv->open(handler->dbenv, path, dbflags, dbmode)) != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s: %s", path, db_strerror(rc));
	return NULL;
    }

    if (!HAVE_SET_LK_EXCLUSIVE && handler->cfg.exclusive) {
	// emulate exclusive lock with an outer txn
	if (begin_transaction(handler, "exclusive txn") != 0) return NULL;
	handler->n_internal_txn++;
    }
    if (begin_transaction(handler, "open txn") != 0) goto abort;

    if (raw_db_open(handler, &handler->dbMeta,    "meta",    DB_BTREE, flags, dbmode) != 0) goto abort;
    if (raw_db_open(handler, &handler->dbKeyname, "keyname", DB_BTREE, flags, dbmode) != 0) goto abort;
    if (raw_db_open(handler, &handler->dbKeyid,   "keyid",   DB_RECNO, flags, dbmode) != 0) goto abort;
    if (raw_db_open(handler, &handler->dbData,    "data",    DB_BTREE, flags, dbmode) != 0) goto abort;
    if (raw_db_open(handler, &handler->dbIsSet,   "is_set",  DB_BTREE, flags, dbmode) != 0) goto abort;

    handler->cfg.entry_size = sizeof(dbats_value); // for db_get_buf_len

#define initcfg(field, defaultval) \
    do { \
	size_t value_len = sizeof(handler->cfg.field); \
	if (is_new) { \
	    handler->cfg.field = (defaultval); \
	    if (raw_db_set(handler, handler->dbMeta, #field, strlen(#field), \
		&handler->cfg.field, value_len) != 0) \
		    goto abort; \
	} else { \
	    int writelock = !handler->cfg.readonly || handler->cfg.exclusive; \
	    if (raw_db_get(handler, handler->dbMeta, #field, strlen(#field), \
		&handler->cfg.field, &value_len, writelock ? DB_RMW : 0) != 0) \
	    { \
		dbats_log(LOG_ERROR, "%s: missing config value: %s", path, #field); \
		goto abort; \
	    } \
	} \
	dbats_log(LOG_CONFIG, "cfg: %s = %u", #field, handler->cfg.field); \
    } while (0)

    initcfg(version,           DBATS_DB_VERSION);
    initcfg(period,            period);
    initcfg(values_per_entry,  values_per_entry);
    initcfg(num_aggs,          1);

    if (handler->cfg.version > DBATS_DB_VERSION) {
	dbats_log(LOG_ERROR, "database version %d > %d", handler->cfg.version,
	    DBATS_DB_VERSION);
	return NULL;
    }

    if (handler->cfg.num_aggs > MAX_NUM_AGGS) {
	dbats_log(LOG_ERROR, "num_aggs %d > %d", handler->cfg.num_aggs,
	    MAX_NUM_AGGS);
	return NULL;
    }

    handler->agg = emalloc(MAX_NUM_AGGS * sizeof(dbats_agg), "handler->agg");
    if (!handler->agg) return NULL;
    handler->tslice = emalloc(MAX_NUM_AGGS * sizeof(dbats_tslice*), "handler->tslice");
    if (!handler->tslice) return NULL;

    handler->cfg.entry_size = handler->cfg.values_per_entry * sizeof(dbats_value);

    // load metadata for aggregates
    if (is_new) {
	memset(&handler->agg[0], 0, sizeof(dbats_agg));
	handler->agg[0].func = DBATS_AGG_NONE;
	handler->agg[0].steps = 1;
	handler->agg[0].period = handler->cfg.period;
    } else {
	size_t value_len;
	char keybuf[32];
	for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	    sprintf(keybuf, "agg%d", agg_id);
	    value_len = sizeof(handler->agg[agg_id]);
	    if ((rc = raw_db_get(handler, handler->dbMeta, keybuf, strlen(keybuf),
		&handler->agg[agg_id], &value_len, 0)) != 0)
	    {
		dbats_log(LOG_ERROR, "error reading %s: %s",
		    keybuf, db_strerror(rc));
		return NULL;
	    } else if (value_len != sizeof(dbats_agg)) {
		dbats_log(LOG_ERROR, "corrupt %s: size %zu != %zu",
		    keybuf, value_len, sizeof(dbats_agg));
		return NULL;
	    }
	}
    }

    // allocate tslice for each aggregate
    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	if (!(handler->tslice[agg_id] = ecalloc(1, sizeof(dbats_tslice), "tslice")))
	    return NULL;
    }

    if (!handler->cfg.readonly) {
	if (handler->cfg.compress) {
	    handler->state_compress = ecalloc(1, sizeof(qlz_state_compress), "qlz_state_compress");
	    if (!handler->state_compress) return NULL;
	}

	rc = raw_db_set(handler, handler->dbMeta, "agg0", strlen("agg0"),
	    &handler->agg[0], sizeof(dbats_agg));
	if (rc != 0) goto abort;
    }

    handler->is_open = 1;
    return handler;

abort:
    abort_transaction(handler);
    return NULL;
}

int dbats_aggregate(dbats_handler *handler, int func, int steps)
{
    int rc;

    DB_BTREE_STAT *stats;
    rc = handler->dbIsSet->stat(handler->dbIsSet, current_txn(handler), &stats, DB_FAST_STAT);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error getting stats for is_set: %s", db_strerror(rc));
	return rc;
    }
    int empty = (stats->bt_pagecnt > stats->bt_empty_pg);
    free(stats);

    if (!empty) {
	dbats_log(LOG_ERROR,
	    "Adding a new aggregation to existing data is not yet supported.");
	return -1;
	// If we're going to allow this, we have to make all references to
	// agg[] and num_aggs read from db instead of memory.
    }

    if (handler->cfg.num_aggs >= MAX_NUM_AGGS) {
	dbats_log(LOG_ERROR, "Too many aggregations (%d)", MAX_NUM_AGGS);
	return ENOMEM;
    }

    int agg_id = handler->cfg.num_aggs++;

    if (!(handler->tslice[agg_id] = ecalloc(1, sizeof(dbats_tslice), "tslice")))
	return errno ? errno : ENOMEM;

    memset(&handler->agg[agg_id], 0, sizeof(dbats_agg));
    handler->agg[agg_id].func = func;
    handler->agg[agg_id].steps = steps;
    handler->agg[agg_id].period = handler->agg[0].period * steps;

    char keybuf[32];
    sprintf(keybuf, "agg%d", agg_id);
    rc = raw_db_set(handler, handler->dbMeta, keybuf, strlen(keybuf),
	&handler->agg[agg_id], sizeof(dbats_agg));
    if (rc != 0) return rc;
    rc = raw_db_set(handler, handler->dbMeta, "num_aggs", strlen("num_aggs"),
	&handler->cfg.num_aggs, sizeof(handler->cfg.num_aggs));
    if (rc != 0) return rc;

    return 0;
}

/*************************************************************************/

// Writes tslice to db (unless db is readonly).
// Also cleans up in-memory tslice, even if db is readonly.
static int dbats_flush_tslice(dbats_handler *handler, int agg_id)
{
    dbats_log(LOG_FINE, "Flush t=%u agg_id=%d", handler->tslice[agg_id]->time, agg_id);
    size_t frag_size = fragsize(handler);
    fragkey_t dbkey;
    char *buf = NULL;
    int rc;
    int changed = 0;

    // Write fragments to the DB
    dbkey.time = handler->tslice[agg_id]->time;
    dbkey.agg_id = agg_id;
    for (int f = 0; f < handler->tslice[agg_id]->num_frags; f++) {
	if (!handler->tslice[agg_id]->frag[f]) continue;
	dbkey.frag_id = f;
	if (handler->cfg.readonly) {
	    // do nothing
	} else if (!handler->tslice[agg_id]->frag_changed[f]) {
	    dbats_log(LOG_VERYFINE, "Skipping frag %d (unchanged)", f);
	} else {
	    if (handler->cfg.compress) {
		if (!buf) {
		    buf = (char*)emalloc(1 + frag_size + QLZ_OVERHEAD, "compression buffer");
		    if (!buf) return errno ? errno : ENOMEM;
		}
		buf[0] = 1; // compression flag
		handler->tslice[agg_id]->frag[f]->compressed = 1;
		size_t compressed_len = qlz_compress(handler->tslice[agg_id]->frag[f],
		    buf+1, frag_size, handler->state_compress);
		dbats_log(LOG_VERYFINE, "Frag %d compression: %zu -> %zu (%.1f %%)",
		    f, frag_size, compressed_len, compressed_len*100.0/frag_size);
		rc = raw_db_set(handler, handler->dbData, &dbkey, sizeof(dbkey),
		    buf, compressed_len + 1);
		if (rc != 0) return rc;
	    } else {
		handler->tslice[agg_id]->frag[f]->compressed = 0;
		dbats_log(LOG_VERYFINE, "Frag %d write: %zu", f, frag_size);
		rc = raw_db_set(handler, handler->dbData, &dbkey, sizeof(dbkey),
		    handler->tslice[agg_id]->frag[f], frag_size);
		if (rc != 0) return rc;
	    }
	    handler->tslice[agg_id]->frag_changed[f] = 0;
	    changed = 1;

	    rc = raw_db_set(handler, handler->dbIsSet, &dbkey, sizeof(dbkey),
		handler->tslice[agg_id]->is_set[f], vec_size(ENTRIES_PER_FRAG));
	    if (rc != 0) return rc;
	}

	if (!handler->cfg.exclusive || !handler->is_open) {
	    if (handler->tslice[agg_id]->frag[f]) {
		free(handler->tslice[agg_id]->frag[f]);
		handler->tslice[agg_id]->frag[f] = 0;
	    }
	    if (handler->tslice[agg_id]->is_set[f]) {
		free(handler->tslice[agg_id]->is_set[f]);
		handler->tslice[agg_id]->is_set[f] = 0;
	    }
	}
    }
    if (buf) free(buf);

    // We read and write agg.times as the last step of a transaction to
    // minimize the time that the lock is held.
    if (changed) {
	// get updated agg times from db
	size_t value_len;
	char keybuf[32];
	sprintf(keybuf, "agg%d", agg_id);
	value_len = sizeof(dbats_agg);
	if ((rc = raw_db_get(handler, handler->dbMeta, keybuf, strlen(keybuf),
	    &handler->agg[agg_id], &value_len, DB_RMW)) != 0)
	{
	    dbats_log(LOG_ERROR, "error getting %s: %s", keybuf, db_strerror(rc));
	    return rc;
	}

	// write new agg times to db if needed
	changed = 0;
	if (handler->agg[agg_id].times.start == 0 ||
	    handler->agg[agg_id].times.start > handler->tslice[agg_id]->time)
	{
	    handler->agg[agg_id].times.start = handler->tslice[agg_id]->time;
	    changed = 1;
	}
	if (handler->agg[agg_id].times.end < handler->tslice[agg_id]->time) {
	    handler->agg[agg_id].times.end = handler->tslice[agg_id]->time;
	    changed = 1;
	}
	if (changed) {
	    rc = raw_db_set(handler, handler->dbMeta, keybuf, strlen(keybuf),
		&handler->agg[agg_id], sizeof(dbats_agg));
	    if (rc != 0) {
		dbats_log(LOG_ERROR, "error updating %s: %s", keybuf, db_strerror(rc));
		return rc;
	    }
	}
    }

    if (!handler->cfg.exclusive) {
	assert(!handler->tslice[agg_id]->is_set[0]);
	memset(handler->tslice[agg_id], 0, sizeof(dbats_tslice));
    }
    return 0;
}

/*************************************************************************/

int dbats_commit(dbats_handler *handler)
{
    if (handler->n_txn <= handler->n_internal_txn) return 0;
    dbats_log(LOG_FINE, "dbats_commit %u", handler->tslice[0]->time);

    if (handler->is_set) {
	int tn = (handler->active_end - handler->active_start) / handler->agg[0].period + 1;
	for (int ti = 0; ti < tn; ti++) {
	    if (handler->is_set[ti]) {
		for (int f = 0; f < handler->tslice[0]->num_frags; f++) {
		    if (handler->is_set[ti][f]) {
			if (handler->tslice[0]->is_set[f] != handler->is_set[ti][f])
			    free(handler->is_set[ti][f]); // shared; avoid double-free
			handler->is_set[ti][f] = 0;
		    }
		}
		free(handler->is_set[ti]);
		handler->is_set[ti] = 0;
	    }
	}
	free(handler->is_set);
	handler->is_set = 0;
    }

    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	int rc = dbats_flush_tslice(handler, agg_id);
	if (rc != 0) {
	    abort_transaction(handler);
	    return rc;
	}
    }

    return commit_transaction(handler);
}

/*************************************************************************/

void dbats_close(dbats_handler *handler)
{
    if (!handler->is_open)
	return;
    handler->is_open = 0;

    dbats_commit(handler);

    if (!HAVE_SET_LK_EXCLUSIVE && handler->cfg.exclusive) {
	commit_transaction(handler); // exclusive txn
	handler->n_internal_txn--;
    }

    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	free(handler->tslice[agg_id]);
	handler->tslice[agg_id] = NULL;
    }

    if (handler->db_get_buf) free(handler->db_get_buf);
    if (handler->agg) free(handler->agg);
    if (handler->tslice) free(handler->tslice);

    //handler->dbMeta->close(handler->dbMeta, 0);
    //handler->dbKeyname->close(handler->dbKeyname, 0);
    //handler->dbKeyid->close(handler->dbKeyid, 0);
    //handler->dbData->close(handler->dbData, 0);
    //handler->dbIsSet->close(handler->dbIsSet, 0);
    handler->dbenv->close(handler->dbenv, 0);

    if (handler->state_decompress)
	free(handler->state_decompress);
    if (handler->state_compress)
	free(handler->state_compress);
    free(handler);
}

/*************************************************************************/

static const time_t time_base = 259200; // 00:00 on first sunday of 1970, UTC

uint32_t dbats_normalize_time(const dbats_handler *handler, int agg_id, uint32_t *t)
{
    *t -= (*t - time_base) % handler->agg[agg_id].period;
    return *t;
}

/*************************************************************************/

static int load_isset(dbats_handler *handler, fragkey_t *dbkey, uint8_t **dest)
{
    static uint8_t *buf = NULL; // can recycle memory across calls
    int rc;
    size_t value_len = vec_size(ENTRIES_PER_FRAG);

    if (!buf && !(buf = emalloc(vec_size(ENTRIES_PER_FRAG), "is_set")))
	return errno ? errno : ENOMEM; // error

    rc = raw_db_get(handler, handler->dbIsSet, dbkey, sizeof(*dbkey),
	buf, &value_len, !handler->cfg.readonly ? DB_RMW : 0);

    dbats_log(LOG_FINE, "load_isset t=%u, agg_id=%u, frag_id=%u: "
	"raw_db_get(is_set) = %d", dbkey->time, dbkey->agg_id, dbkey->frag_id, rc);

    if (rc == DB_NOTFOUND) {
	if (dbkey->time == handler->tslice[dbkey->agg_id]->time) {
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

    if (dbkey->agg_id == 0 && handler->active_last_data < dbkey->time)
	handler->active_last_data = dbkey->time;

    return 0; // ok
}

static int load_frag(dbats_handler *handler, uint32_t t, int agg_id,
    uint32_t frag_id)
{
    // load fragment
    fragkey_t dbkey;
    dbkey.time = t;
    dbkey.agg_id = agg_id;
    dbkey.frag_id = frag_id;
    int rc;

    assert(!handler->tslice[agg_id]->is_set[frag_id]);
    rc = load_isset(handler, &dbkey, &handler->tslice[agg_id]->is_set[frag_id]);
    if (rc == DB_NOTFOUND) return 0; // no match
    if (rc != 0) return rc; // error

    if (handler->db_get_buf_len < fragsize(handler) + QLZ_OVERHEAD) {
	if (handler->db_get_buf) free(handler->db_get_buf);
	handler->db_get_buf_len = fragsize(handler) + QLZ_OVERHEAD;
	handler->db_get_buf = emalloc(handler->db_get_buf_len, "get buffer");
	if (!handler->db_get_buf) return errno ? errno : ENOMEM;
    }

    size_t value_len = handler->db_get_buf_len;
    rc = raw_db_get(handler, handler->dbData, &dbkey, sizeof(dbkey),
	handler->db_get_buf, &value_len, !handler->cfg.readonly ? DB_RMW : 0);
    dbats_log(LOG_FINE, "load_frag t=%u, agg_id=%u, frag_id=%u: "
	"raw_db_get(frag) = %d", t, agg_id, frag_id, rc);
    if (rc != 0) {
	if (rc == DB_NOTFOUND) return 0; // no match
	return rc; // error
    }

    int compressed = ((dbats_frag*)handler->db_get_buf)->compressed;
    size_t len = !compressed ? fragsize(handler) :
	qlz_size_decompressed((void*)(handler->db_get_buf+1));
    void *ptr = malloc(len);
    if (!ptr) {
	rc = errno;
	dbats_log(LOG_ERROR, "Can't allocate %zu bytes for frag "
	    "t=%u, agg_id=%u, frag_id=%u", len, t, agg_id, frag_id);
	return rc; // error
    }

    if (compressed) {
	// decompress fragment
	if (!handler->state_decompress) {
	    handler->state_decompress = ecalloc(1, sizeof(qlz_state_decompress), "qlz_state_decompress");
	    if (!handler->state_decompress) return errno ? errno : ENOMEM;
	}
	len = qlz_decompress((void*)(handler->db_get_buf+1), ptr, handler->state_decompress);
	// assert(len == fragsize(handler));
	dbats_log(LOG_VERYFINE, "decompressed frag t=%u, agg_id=%u, frag_id=%u: "
	    "%u -> %u (%.1f%%)",
	    t, agg_id, frag_id, value_len, len, len*100.0/value_len);

    } else {
	// copy fragment
	// XXX TODO: don't memcpy; get directly into ptr
	memcpy(ptr, handler->db_get_buf, len);
	dbats_log(LOG_VERYFINE, "copied frag t=%u, agg_id=%u, frag_id=%u",
	    t, agg_id, frag_id);
    }

    handler->tslice[agg_id]->frag[frag_id] = ptr;

    dbats_tslice *tslice = handler->tslice[agg_id];
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;

    return 0;
}

// Instantiate handler->is_set[*][frag_id]
static int instantiate_isset_frags(dbats_handler *handler, int frag_id)
{
    if (handler->cfg.readonly)
	return 0; // handler->is_set is never needed in readonly mode

    int tn = (handler->active_end - handler->active_start) / handler->agg[0].period + 1;

    if (!handler->is_set) {
	handler->is_set = ecalloc(tn, sizeof(uint8_t**), "handler->is_set");
	if (!handler->is_set) return errno ? errno : ENOMEM;
    }

    uint32_t t = handler->active_start;
    for (int ti = 0; ti < tn; ti++, t += handler->agg[0].period) {
	fragkey_t dbkey;
	dbkey.time = t;
	dbkey.agg_id = 0;
	if (!handler->is_set[ti]) {
	    handler->is_set[ti] = ecalloc(MAX_NUM_FRAGS, sizeof(uint8_t*), "handler->is_set[ti]");
	    if (!handler->is_set[ti]) return errno ? errno : ENOMEM;
	}

	if (t == handler->tslice[0]->time && handler->tslice[0]->is_set[frag_id]) {
	    assert(!handler->is_set[ti][frag_id]);
	    handler->is_set[ti][frag_id] = handler->tslice[0]->is_set[frag_id]; // share
	} else if (!handler->is_set[ti][frag_id]) {
	    dbkey.frag_id = frag_id;
	    assert(!handler->is_set[ti][frag_id]);
	    int rc = load_isset(handler, &dbkey, &handler->is_set[ti][frag_id]);
	    if (rc != 0 && rc != DB_NOTFOUND)
		return rc; // error
	    if (t == handler->tslice[0]->time) {
		assert(!handler->tslice[0]->is_set[frag_id]);
		handler->tslice[0]->is_set[frag_id] = handler->is_set[ti][frag_id]; // share
	    }
	}
    }
    return 0;
}

int dbats_num_keys(dbats_handler *handler, uint32_t *num_keys)
{
    int rc;
    DB_BTREE_STAT *stats;

    rc = handler->dbKeyid->stat(handler->dbKeyid, current_txn(handler), &stats, DB_FAST_STAT);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error getting stats for keys: %s", db_strerror(rc));
	return rc;
    }

    *num_keys = stats->bt_nkeys;
    free(stats);
    return 0;
}

int dbats_select_time(dbats_handler *handler, uint32_t time_value, uint32_t flags)
{
    int rc;

    dbats_log(LOG_FINE, "select_time %u", time_value);

    if ((rc = dbats_commit(handler)) != 0)
	return rc;

    if ((rc = begin_transaction(handler, "tslice txn")) != 0)
	return rc;

    handler->preload = !!(flags & DBATS_PRELOAD);

    uint32_t min_time = 0;
    uint32_t max_time = 0;

    if (handler->serialize) {
	// Lock "agg0" to enforce one writer at a time.
	// Timeslice transactions are sufficiently complex that they end up
	// being serialized anyway.  By serializing up front, we avoid all
	// deadlocks and don't need sub-transactions around dbats_get() and
	// dbats_set().
	int timeout = 10;
	const char *keybuf = "agg0";
	while (1) {
	    size_t value_len;
	    value_len = sizeof(dbats_agg);
	    int dbflags = handler->cfg.readonly ? 0 : DB_RMW;
	    rc = raw_db_get(handler, handler->dbMeta, keybuf, strlen(keybuf),
		&handler->agg[0], &value_len, dbflags);
	    if (rc == 0) break;
	    if (rc != DB_LOCK_DEADLOCK || --timeout <= 0) {
		dbats_log(LOG_ERROR, "error getting %s: %s", keybuf, db_strerror(rc));
		return rc;
	    }
	    dbats_log(LOG_FINE, "select_time %u: get %s: deadlock, timeout=%d",
		time_value, keybuf, timeout);
	}
	dbats_log(LOG_FINE, "select_time %u: got %s", time_value, keybuf);
    }

    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	dbats_tslice *tslice = handler->tslice[agg_id];
	uint32_t t = time_value;

	dbats_normalize_time(handler, agg_id, &t);

	if (handler->cfg.exclusive) {
	    if (tslice->time == t) {
		dbats_log(LOG_FINE, "select_time %u, agg_id=%d: already loaded", t, agg_id);
		continue;
	    }
	    // free obsolete fragments
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
	    assert(!tslice->is_set[0]);
	    memset(tslice, 0, sizeof(dbats_tslice));
	}

	tslice->time = t;
	if (agg_id == 0)
	    handler->active_last_data = t;

	if (!min_time || t < min_time)
	    min_time = t;
	t += handler->agg[agg_id].period - handler->agg[0].period;
	if (t > max_time)
	    max_time = t;
    }
    handler->active_start = min_time;
    handler->active_end = max_time;

    if (!handler->preload)
	return 0;

    uint32_t num_keys;
    if ((rc = dbats_num_keys(handler, &num_keys)) != 0)
	return rc;

    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	dbats_tslice *tslice = handler->tslice[agg_id];
	uint32_t t = tslice->time;
	int loaded = 0;

	tslice->num_frags = (num_keys + ENTRIES_PER_FRAG - 1) / ENTRIES_PER_FRAG;
	for (uint32_t frag_id = 0; frag_id < tslice->num_frags; frag_id++) {
	    if ((rc = load_frag(handler, t, agg_id, frag_id)) != 0)
		return rc;
	    if (tslice->frag[frag_id])
		loaded++;
	}
	dbats_log(LOG_FINE, "select_time %u: agg_id=%d, loaded %u/%u fragments",
	    time_value, agg_id, loaded, tslice->num_frags);
    }

    for (int frag_id = 0; frag_id < handler->tslice[0]->num_frags; frag_id++) {
	instantiate_isset_frags(handler, frag_id);
    }

    return 0;
}

/*************************************************************************/

int dbats_get_key_id(dbats_handler *handler, const char *key,
    uint32_t *key_id_p, uint32_t flags)
{
    size_t keylen = strlen(key);
    size_t len = sizeof(*key_id_p);
    int rc;

    rc = raw_db_get(handler, handler->dbKeyname, key, keylen, key_id_p, &len,
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
	DBT_out(dbt_keyrecno, &recno, sizeof(recno)); // put() will fill in recno
	rc = handler->dbKeyid->put(handler->dbKeyid, current_txn(handler), &dbt_keyrecno, &dbt_keyname, DB_APPEND);
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "Error creating keyid for %s: %s", key, db_strerror(rc));
	    return rc;
	}
	*key_id_p = recno - 1;
	if (*key_id_p >= MAX_NUM_FRAGS * ENTRIES_PER_FRAG) {
	    dbats_log(LOG_ERROR, "Out of space for key %s", key);
	    rc = handler->dbKeyid->del(handler->dbKeyid, current_txn(handler), &dbt_keyrecno, 0);
	    return ENOMEM;
	}

	DBT_in(dbt_keyid, key_id_p, sizeof(*key_id_p));
	rc = handler->dbKeyname->put(handler->dbKeyname, current_txn(handler), &dbt_keyname, &dbt_keyid, DB_NOOVERWRITE);
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "Error creating keyname %s -> %u: %s", key, *key_id_p, db_strerror(rc));
	    return rc;
	}
	dbats_log(LOG_FINEST, "Assigned key #%u: %s", *key_id_p, key);

	return 0;
    }
}

int dbats_get_key_name(dbats_handler *handler, uint32_t key_id, char *namebuf)
{
    int rc;
    db_recno_t recno = key_id + 1;
    uint32_t value_len = DBATS_KEYLEN - 1;

    rc = raw_db_get(handler, handler->dbKeyid, &recno, sizeof(recno),
	namebuf, &value_len, DB_READ_COMMITTED);
    if (rc != 0) return rc;
    namebuf[value_len] = '\0';
    return 0;
}

/*************************************************************************/

static int instantiate_frag_func(dbats_handler *handler, int agg_id, uint32_t frag_id)
{
    int rc;
    dbats_tslice *tslice = handler->tslice[agg_id];

    if (!handler->preload || handler->cfg.readonly) {
	// Try to load the fragment.
	rc = load_frag(handler, tslice->time, agg_id, frag_id);
	if (rc != 0) // error
	    return rc;
	if (tslice->frag[frag_id]) { // found it
	    if (agg_id == 0)
		rc = instantiate_isset_frags(handler, frag_id);
	    return rc;
	}
	if (handler->cfg.readonly) // didn't find it and can't create it
	    return DB_NOTFOUND; // fragment not found
    }

    // Allocate a new fragment.
    dbats_log(LOG_VERYFINE, "grow tslice for time %u, agg_id %d, frag_id %u",
	tslice->time, agg_id, frag_id);

    size_t len = fragsize(handler);
    if (!(tslice->frag[frag_id] = ecalloc(1, len, "tslice->frag[frag_id]"))) {
	return errno ? errno : ENOMEM;
    }

    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;
    if (agg_id == 0) {
	rc = instantiate_isset_frags(handler, frag_id);
	if (rc != 0) return rc;
    }

    dbats_log(LOG_VERYFINE, "Grew tslice to %u elements",
	tslice->num_frags * ENTRIES_PER_FRAG);

    return 0;
}

// inline version handles the common case without a function call
static inline int instantiate_frag(dbats_handler *handler, int agg_id, uint32_t frag_id)
{
    return handler->tslice[agg_id]->frag[frag_id] ?
	0 : // We already have the fragment.
	instantiate_frag_func(handler, agg_id, frag_id);
}

/*************************************************************************/

int dbats_set(dbats_handler *handler, uint32_t key_id, const dbats_value *valuep)
{
    dbats_log(LOG_VERYFINE, "dbats_set %u #%u = %" PRIval, handler->tslice[0]->time, key_id, valuep[0]); // XXX
    int rc;

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

    if (!handler->is_open || handler->cfg.readonly) {
	dbats_log(LOG_ERROR, "is_open=%d, readonly=%d", handler->is_open, handler->cfg.readonly);
	return -1;
    }

retry:
    if (!handler->serialize)
	begin_transaction(handler, "set txn");

    if ((rc = instantiate_frag(handler, 0, frag_id)) != 0) {
	if (!handler->serialize) {
	    abort_transaction(handler); // set txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }

    uint8_t was_set = vec_test(handler->tslice[0]->is_set[frag_id], offset);
    dbats_value *oldvaluep = valueptr(handler, 0, frag_id, offset);

    // For each agg_id, aggregate *valuep into tslice[agg_id].
    for (int agg_id = 1; agg_id < handler->cfg.num_aggs; agg_id++) {
	if ((rc = instantiate_frag(handler, agg_id, frag_id)) != 0) {
	    if (!handler->serialize) {
		abort_transaction(handler); // set txn
		if (rc == DB_LOCK_DEADLOCK) goto retry;
	    }
	    return rc;
	}

	uint8_t changed = 0;
	uint8_t failed = 0;
	dbats_value *aggval = valueptr(handler, agg_id, frag_id, offset);
	uint32_t aggstart = handler->tslice[agg_id]->time;
	uint32_t aggend = min(handler->active_last_data,
	    aggstart + handler->agg[agg_id].period - handler->agg[0].period);

	// Count the number of steps contributing to the aggregate.
	int n = 0;
	{
	    int ti = (aggstart - handler->active_start) / handler->agg[0].period;
	    uint32_t t = aggstart;
	    while (t <= aggend) {
		if (t == handler->tslice[0]->time ||
		    vec_test(handler->is_set[ti][frag_id], offset))
			n++;
		ti++;
		t += handler->agg[0].period;
	    }
	}

	dbats_log(LOG_FINEST, "agg %d: aggval=%" PRIval " n=%d",
	    agg_id, aggval[0], n); // XXX

	for (int i = 0; i < handler->cfg.values_per_entry; i++) {
	    if (was_set && valuep[i] == oldvaluep[i]) {
		continue; // value did not change; no need to change agg value
	    }
	    switch (handler->agg[agg_id].func) {
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
		if (handler->tslice[0]->time >= handler->active_last_data) {
		    // common case: value is latest ever seen
		    aggval[i] = valuep[i];
		    changed = 1;
		} else {
		    // find time of last sample for this key within agg period
		    uint32_t t = aggend;
		    int ti = (t - handler->active_start) / handler->agg[0].period;
		    while (t >= aggstart) {
			if (t == handler->tslice[0]->time) {
			    // the new value is the last
			    aggval[i] = valuep[i];
			    changed = 1;
			    break;
			}
			if (vec_test(handler->is_set[ti][frag_id], offset))
			    // an existing value is later than the new value
			    break;
			t -= handler->agg[0].period;
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
		if (handler->agg[agg_id].func == DBATS_AGG_AVG)
		    sprintf(aggbuf, "%f", ((double*)aggval)[0]);
		else
		    sprintf(aggbuf, "%" PRIval, aggval[0]);
		dbats_log(level, "%s set value agg_id=%d frag_id=%u "
		    "offset=%" PRIu32 " value_len=%u aggval=%s",
		    failed ? "Failed to" : "Successfully",
		    agg_id, frag_id, offset, handler->cfg.entry_size, aggbuf);
	    }
	}
	if (changed) {
	    // XXX if (n >= xff * steps)
	    vec_set(handler->tslice[agg_id]->is_set[frag_id], offset);
	    handler->tslice[agg_id]->frag_changed[frag_id] = 1;
	} else if (failed) {
	    if (!handler->serialize)
		abort_transaction(handler); // set txn
	    return failed;
	}
    }

    // Set value at agg_id 0 (after aggregations because aggregations need
    // both old and new values)
    memcpy(oldvaluep, valuep, handler->cfg.entry_size);
    vec_set(handler->tslice[0]->is_set[frag_id], offset);
    handler->tslice[0]->frag_changed[frag_id] = 1;
    dbats_log(LOG_VERYFINE, "Succesfully set value "
	"agg_id=%d frag_id=%u offset=%" PRIu32 " value_len=%u value=%" PRIval,
	0, frag_id, offset, handler->cfg.entry_size, valuep[0]);

    if (!handler->serialize)
	commit_transaction(handler); // set txn
    return 0;
}

int dbats_set_by_key(dbats_handler *handler, const char *key,
    const dbats_value *valuep, int flags)
{
    int rc;
    uint32_t key_id;

    if ((rc = dbats_get_key_id(handler, key, &key_id, flags)) != 0)
	return rc;
    return dbats_set(handler, key_id, valuep);
}

/*************************************************************************/

int dbats_get(dbats_handler *handler, uint32_t key_id,
    const dbats_value **valuepp, int agg_id)
{
    int rc;
    dbats_tslice *tslice = handler->tslice[agg_id];

    if (!handler->is_open) {
	*valuepp = NULL;
	return -1;
    }

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

retry:
    if (!handler->serialize)
	begin_transaction(handler, "get txn");

    if ((rc = instantiate_frag(handler, agg_id, frag_id)) == 0) {
	if (!vec_test(tslice->is_set[frag_id], offset)) {
	    char keyname[DBATS_KEYLEN] = "";
	    dbats_get_key_name(handler, key_id, keyname);
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, agg_id, keyname);
	    *valuepp = NULL;
	    if (!handler->serialize)
		commit_transaction(handler); // get txn
	    return DB_NOTFOUND;
	}
	if (handler->agg[agg_id].func == DBATS_AGG_AVG) {
	    double *dval = (double*)valueptr(handler, agg_id, frag_id, offset);
	    static dbats_value *avg_buf = NULL;
	    if (!avg_buf) {
		avg_buf = emalloc(handler->cfg.entry_size * handler->cfg.values_per_entry, "avg_buf");
		if (!avg_buf) {
		    if (!handler->serialize)
			abort_transaction(handler); // get txn
		    return errno ? errno : ENOMEM;
		}
	    }
	    for (int i = 0; i < handler->cfg.values_per_entry; i++)
		avg_buf[i] = dval[i] + 0.5;
	    *valuepp = avg_buf;
	} else {
	    *valuepp = valueptr(handler, agg_id, frag_id, offset);
	}
	dbats_log(LOG_VERYFINE, "Succesfully read value [offset=%" PRIu32 "][value_len=%u]",
	    offset, handler->cfg.entry_size);
	if (!handler->serialize)
	    commit_transaction(handler); // get txn
	return 0;

    } else if (rc == DB_NOTFOUND) {
	char keyname[DBATS_KEYLEN] = "";
	dbats_get_key_name(handler, key_id, keyname);
	dbats_log(LOG_WARNING, "Value unset (f): %u %d %s",
	    tslice->time, agg_id, keyname);
	*valuepp = NULL;
	if (!handler->serialize)
	    commit_transaction(handler); // get txn
	return rc;

    } else {
	if (!handler->serialize) {
	    abort_transaction(handler); // get txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }
}

int dbats_get_double(dbats_handler *handler, uint32_t key_id,
    const double **valuepp, int agg_id)
{
    int rc;
    dbats_tslice *tslice = handler->tslice[agg_id];

    if (!handler->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if (handler->agg[agg_id].func != DBATS_AGG_AVG) {
	dbats_log(LOG_ERROR, "Aggregation %d is not a double", agg_id);
	*valuepp = NULL;
	return -1;
    }

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

retry:
    if (!handler->serialize)
	begin_transaction(handler, "get txn");

    if ((rc = instantiate_frag(handler, agg_id, frag_id)) == 0) {
	if (!vec_test(tslice->is_set[frag_id], offset)) {
	    char keyname[DBATS_KEYLEN] = "";
	    dbats_get_key_name(handler, key_id, keyname);
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, agg_id, keyname);
	    *valuepp = NULL;
	    if (!handler->serialize)
		commit_transaction(handler); // get txn
	    return DB_NOTFOUND;
	}
	*valuepp = (double*)valueptr(handler, agg_id, frag_id, offset);
	dbats_log(LOG_VERYFINE, "Succesfully read value [offset=%" PRIu32 "][value_len=%u]",
	    offset, handler->cfg.entry_size);
	if (!handler->serialize)
	    commit_transaction(handler); // get txn
	return 0;

    } else if (rc == DB_NOTFOUND) {
	char keyname[DBATS_KEYLEN] = "";
	dbats_get_key_name(handler, key_id, keyname);
	dbats_log(LOG_WARNING, "Value unset (f): %u %d %s",
	    tslice->time, agg_id, keyname);
	*valuepp = NULL;
	if (!handler->serialize)
	    commit_transaction(handler); // get txn
	return rc;

    } else {
	if (!handler->serialize) {
	    abort_transaction(handler); // get txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }
}

int dbats_get_by_key(dbats_handler *handler, const char *key,
    const dbats_value **valuepp, int agg_id)
{
    int rc;
    uint32_t key_id;

    if ((rc = dbats_get_key_id(handler, key, &key_id, 0)) != 0) {
	return rc;
    }
    return dbats_get(handler, key_id, valuepp, agg_id);
}

/*************************************************************************/

int dbats_walk_keyname_start(dbats_handler *handler)
{
    int rc;

    if ((rc = handler->dbKeyname->cursor(handler->dbKeyname, NULL, &handler->keyname_cursor, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyname_start: %s", db_strerror(rc));
	return -1;
    }
    return 0;
}

int dbats_walk_keyname_next(dbats_handler *handler, uint32_t *key_id_p,
    char *namebuf)
{
    int rc;
    DBT_out(dbt_keyid, key_id_p, sizeof(*key_id_p));
    DBT_out(dbt_keyname, namebuf, DBATS_KEYLEN-1);
    rc = handler->keyname_cursor->get(handler->keyname_cursor,
	&dbt_keyname, &dbt_keyid, DB_NEXT | DB_READ_COMMITTED);
    if (rc != 0) {
	if (rc != DB_NOTFOUND)
	    dbats_log(LOG_ERROR, "Error in dbats_walk_keyname_next: %s", db_strerror(rc));
	return -1;
    }
    if (namebuf)
	namebuf[dbt_keyname.size] = '\0';
    return 0;
}

int dbats_walk_keyname_end(dbats_handler *handler)
{
    int rc;

    if ((rc = handler->keyname_cursor->close(handler->keyname_cursor)) != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyname_end: %s", db_strerror(rc));
	return -1;
    }
    return 0;
}

/*************************************************************************/

int dbats_walk_keyid_start(dbats_handler *handler)
{
    int rc;

    if ((rc = handler->dbKeyid->cursor(handler->dbKeyid, NULL, &handler->keyid_cursor, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyid_start: %s", db_strerror(rc));
	return -1;
    }
    return 0;
}

int dbats_walk_keyid_next(dbats_handler *handler, uint32_t *key_id_p,
    char *namebuf)
{
    int rc;
    db_recno_t recno;

    DBT_out(dbt_keyrecno, &recno, sizeof(recno));
    DBT_out(dbt_keyname, namebuf, DBATS_KEYLEN-1);
    rc = handler->keyid_cursor->get(handler->keyid_cursor,
	&dbt_keyrecno, &dbt_keyname, DB_NEXT | DB_READ_COMMITTED);
    if (rc != 0) {
	if (rc != DB_NOTFOUND)
	    dbats_log(LOG_ERROR, "Error in dbats_walk_keyid_next: %s", db_strerror(rc));
	return -1;
    }
    *key_id_p = recno - 1;
    if (namebuf)
	namebuf[dbt_keyname.size] = '\0';
    return 0;
}

int dbats_walk_keyid_end(dbats_handler *handler)
{
    int rc;

    if ((rc = handler->keyid_cursor->close(handler->keyid_cursor)) != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyid_end: %s", db_strerror(rc));
	return -1;
    }
    return 0;
}

/*************************************************************************/

const volatile dbats_config *dbats_get_config(dbats_handler *handler)
    { return &handler->cfg; }

const volatile dbats_agg *dbats_get_agg(dbats_handler *handler, int agg_id)
    { return &handler->agg[agg_id]; }

/*************************************************************************/

void dbats_stat_print(const dbats_handler *handler) {
    int rc;
    
    if ((rc = handler->dbMeta->stat_print(handler->dbMeta, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "Error while dumping DB stats for Meta [%s]", db_strerror(rc));
    if ((rc = handler->dbKeyname->stat_print(handler->dbKeyname, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "Error while dumping DB stats for Keyname [%s]", db_strerror(rc));
    if ((rc = handler->dbKeyid->stat_print(handler->dbKeyid, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "Error while dumping DB stats for Keyid [%s]", db_strerror(rc));
    if ((rc = handler->dbData->stat_print(handler->dbData, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "Error while dumping DB stats for Data [%s]", db_strerror(rc));
    if ((rc = handler->dbIsSet->stat_print(handler->dbIsSet, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "Error while dumping DB stats for IsSet [%s]", db_strerror(rc));
}

