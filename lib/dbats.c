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
#define KEYINFO_PER_BLK     10000 // number of key_info in a block
#define MAX_NUM_KEYINFOBLK  16384 // max number of key_info blocks
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
#define vec_test(vec, i)   (vec[(i)/8] & (1<<((i)%8)))    // test the ith bit

#define valueptr(handler, agg_id, frag_id, offset) \
    ((dbats_value*)(&handler->tslice[agg_id]->frag[frag_id]->data[offset * handler->cfg.entry_size]))

// "Fragment", containing a large subset of entries for a timeslice
typedef struct dbats_frag {
    uint8_t compressed; // is data in db compressed?
    uint8_t data[]; // values (C99 flexible array member)
} dbats_frag;

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

typedef struct dbats_key_info {
    const char *key;
    uint32_t key_id;               // id of key/column
    uint32_t frag_id;              // fragment within timeslice
    uint32_t offset;               // index within fragment
} dbats_key_info_t;

// key_info as stored in db
typedef struct {
    uint32_t key_id;
} raw_key_info_t;

struct dbats_handler {
    dbats_config cfg;
    uint8_t is_open;
    uint8_t preload;                   // load frags when tslice is selected
    void *state_compress;
    void *state_decompress;
    DB_ENV *dbenv;                     // DB environment
    DB *dbMeta;                        // config parameters
    DB *dbKeys;                        // key name -> key_info
    DB *dbData;                        // {time, agg_id, frag_id} -> value fragment
    DB *dbIsSet;                       // {time, agg_id, frag_id} -> is_set fragment
    DBC *keyname_cursor;               // for iterating over key names
    uint32_t keyid_walker;             // for iterating over key ids
    dbats_tslice **tslice;             // a tslice for each aggregation level
    dbats_agg *agg;                    // parameters for each aggregation level
    dbats_key_info_t **key_info_block;
    uint8_t *db_get_buf;
    size_t db_get_buf_len;
    DB_TXN *txn;                       // current transaction
    DB_TXN *outer_txn;                 // used to emulate set_lk_exclusive()
    uint32_t active_start;             // first timestamp in active period
    uint32_t active_end;               // last timestamp in active period
    uint8_t ***is_set;                 // bitvectors: which cols have values
};

const char *dbats_agg_func_label[] = {
    "data", "min", "max", "avg", "last", "sum"
};

/************************************************************************
 * utilities
 ************************************************************************/

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
    int rc;
    if ((rc = handler->dbenv->txn_begin(handler->dbenv, handler->outer_txn, &handler->txn, DB_TXN_BULK)) != 0) {
	dbats_log(LOG_ERROR, "begin transaction: %s", db_strerror(rc));
	return rc;
    }
    handler->txn->set_name(handler->txn, name);
    return 0;
}

static inline int commit_transaction(dbats_handler *handler)
{
    if (handler->cfg.no_txn) return 0;
    int rc;
    if ((rc = handler->txn->commit(handler->txn, 0)) != 0)
	dbats_log(LOG_ERROR, "commit transaction: %s", db_strerror(rc));
    handler->txn = NULL;
    return rc;
}

static inline int abort_transaction(dbats_handler *handler)
{
    if (handler->cfg.no_txn) return 0;
    int rc;
    if ((rc = handler->txn->abort(handler->txn)) != 0)
	dbats_log(LOG_ERROR, "abort transaction: %s", db_strerror(rc));
    handler->txn = NULL;
    return rc;
}

static int raw_db_open(dbats_handler *handler, DB **dbp, const char *envpath,
    const char *dbname, uint32_t flags, int mode)
{
    int rc;

    if ((rc = db_create(dbp, handler->dbenv, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error creating DB handler %s/%s: %s",
	    envpath, dbname, db_strerror(rc));
	return rc;
    }

    if (handler->cfg.exclusive) {
#if HAVE_SET_LK_EXCLUSIVE
	if ((rc = (*dbp)->set_lk_exclusive(*dbp, 1)) != 0) {
	    dbats_log(LOG_ERROR, "Error obtaining exclusive lock on %s/%s: %s",
		envpath, dbname, db_strerror(rc));
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

    rc = (*dbp)->open(*dbp, handler->txn, dbname, NULL, DB_BTREE, dbflags, mode);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s/%s (mode=%o): %s",
	    envpath, dbname, mode, db_strerror(rc));
	(*dbp)->close(*dbp, 0);
	return rc;
    }
    return 0;
}

static int raw_db_set(const dbats_handler *handler, DB *db,
    const void *key, uint32_t key_len,
    const void *value, uint32_t value_len)
{
    DBT key_data, data;
    int rc;

    if (db == handler->dbData) {
	fragkey_t *fragkey = (fragkey_t*)key;
	dbats_log(LOG_INFO, "raw_db_set Data: t=%u agg=%d frag=%u, compress=%d, len=%u",
	    fragkey->time, fragkey->agg_id, fragkey->frag_id, *(uint8_t*)value, value_len);
    }

    if (handler->cfg.readonly) {
	const char *dbname;
	db->get_dbname(db, &dbname, NULL);
	dbats_log(LOG_WARNING, "Unable to set value in database \"%s\" "
	    "(read-only mode)", dbname);
	return EPERM;
    }

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));
    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    key_data.flags = DB_DBT_USERMEM;
    data.data = (void*)value; // assumption: DB won't write to *data.data
    data.size = value_len;
    data.flags = DB_DBT_USERMEM;

    if ((rc = db->put(db, handler->txn, &key_data, &data, 0)) != 0) {
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
    int writelock)
{
    DBT key_data, data;
    int rc;

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));

    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    key_data.flags = DB_DBT_USERMEM;
    data.data = value;
    data.ulen = *value_len;
    data.flags = DB_DBT_USERMEM;
    uint32_t dbflags = 0;
    if (writelock) dbflags |= DB_RMW;
    if ((rc = db->get(db, handler->txn, &key_data, &data, dbflags)) == 0) {
	*value_len = data.size;
	return 0;
    }
    if (rc != DB_NOTFOUND)
	dbats_log(LOG_INFO, "raw_db_get failed: %s", db_strerror(rc));
    if (rc == DB_BUFFER_SMALL) {
	dbats_log(LOG_WARNING, "raw_db_get: had %" PRIu32 ", needed %" PRIu32,
	    data.ulen, data.size);
    }
    return rc;
}

/*
static void raw_db_delete(const dbats_handler *handler, DB *db,
    const void *key, uint32_t key_len)
{
    DBT key_data;

    if (handler->cfg.readonly) {
	dbats_log(LOG_WARNING, "Unable to delete value (read-only mode)");
	return EPERM;
    }

    memset(&key_data, 0, sizeof(key_data));
    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    key_data.flags = DB_DBT_USERMEM;

    if (db->del(db, NULL, &key_data, 0) != 0)
	dbats_log(LOG_WARNING, "Error while deleting key");
}
*/

/*************************************************************************/

static inline dbats_key_info_t *find_key(dbats_handler *handler, uint32_t key_id)
{
    uint32_t block = key_id / KEYINFO_PER_BLK;
    uint32_t offset = key_id % KEYINFO_PER_BLK;
    return &handler->key_info_block[block][offset];
}

// allocate a key_info block if needed
static dbats_key_info_t *new_key(dbats_handler *handler,
    uint32_t key_id, char *keycopy)
{
    uint32_t block = key_id / KEYINFO_PER_BLK;
    uint32_t offset = key_id % KEYINFO_PER_BLK;

    if (!handler->key_info_block[block]) {
	handler->key_info_block[block] =
	    ecalloc(KEYINFO_PER_BLK, sizeof(dbats_key_info_t), "key_info_block");
	if (!handler->key_info_block[block])
	    return NULL;
    }
    dbats_key_info_t *dkip = &handler->key_info_block[block][offset];
    dkip->key = keycopy;
    dkip->key_id = key_id;
    dkip->frag_id = key_id / ENTRIES_PER_FRAG;
    dkip->offset = key_id % ENTRIES_PER_FRAG;
    return dkip;
}

// Warning: never open the same db more than once in the same process, because
// there must be only one DB_ENV per db per process.
// http://docs.oracle.com/cd/E17076_02/html/programmer_reference/transapp_app.html
dbats_handler *dbats_open(const char *path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags)
{
    dbats_log(LOG_INFO, "%s", db_full_version(NULL, NULL, NULL, NULL, NULL));

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
	} else {
	    // new database dir failed
	    dbats_log(LOG_ERROR, "Error creating %s: %s",
		path, strerror(errno));
	    return NULL;
	}
    }

    if ((rc = handler->dbenv->set_lk_max_locks(handler->dbenv, 20000)) != 0) {
	dbats_log(LOG_ERROR, "Error in set_lk_max_locks: %s: %s", path, db_strerror(rc));
	return NULL;
    }

    if ((rc = handler->dbenv->set_lk_max_objects(handler->dbenv, 20000)) != 0) {
	dbats_log(LOG_ERROR, "Error in set_lk_max_objects: %s: %s", path, db_strerror(rc));
	return NULL;
    }

    if ((rc = handler->dbenv->open(handler->dbenv, path, dbflags, dbmode)) != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s: %s", path, db_strerror(rc));
	return NULL;
    }

    if (begin_transaction(handler, "outer txn") != 0)
	return NULL;

    if (raw_db_open(handler, &handler->dbMeta,  path, "meta",   flags, dbmode) != 0) goto abort;
    if (raw_db_open(handler, &handler->dbKeys,  path, "keys",   flags, dbmode) != 0) goto abort;
    if (raw_db_open(handler, &handler->dbData,  path, "data",   flags, dbmode) != 0) goto abort;
    if (raw_db_open(handler, &handler->dbIsSet, path, "is_set", flags, dbmode) != 0) goto abort;

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
		&handler->cfg.field, &value_len, writelock) != 0) \
	    { \
		dbats_log(LOG_ERROR, "%s: missing config value: %s", path, #field); \
		goto abort; \
	    } \
	} \
	dbats_log(LOG_INFO, "cfg: %s = %u", #field, handler->cfg.field); \
    } while (0)

    initcfg(version,           DBATS_DB_VERSION);
    initcfg(num_keys,          0);
    initcfg(period,            period);
    initcfg(values_per_entry,  values_per_entry);
    initcfg(num_aggs,          1);

    if (!HAVE_SET_LK_EXCLUSIVE && handler->cfg.exclusive) {
	// emulate exclusive lock by leaving this transaction open
	handler->outer_txn = handler->txn;
	handler->txn = NULL;
    } else {
	if (commit_transaction(handler) != 0) goto abort;
    }

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
    handler->key_info_block = ecalloc(MAX_NUM_KEYINFOBLK, sizeof(dbats_key_info_t*), "handler->key_info_block");
    if (!handler->key_info_block) return NULL;

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
	    if (raw_db_get(handler, handler->dbMeta, keybuf, strlen(keybuf),
		&handler->agg[agg_id], &value_len, 0) != 0)
	    {
		dbats_log(LOG_ERROR, "missing aggregation config %d", agg_id);
		return NULL;
	    } else if (value_len != sizeof(dbats_agg)) {
		dbats_log(LOG_ERROR, "corrupt aggregation config: size %zu != %zu",
		    value_len, sizeof(dbats_agg));
		return NULL;
	    }
	}
    }

    // allocate tslice for each aggregate
    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	if (!(handler->tslice[agg_id] = ecalloc(1, sizeof(dbats_tslice), "tslice")))
	    return NULL;
    }

    if (handler->cfg.compress && !handler->cfg.readonly) {
	handler->state_compress = ecalloc(1, sizeof(qlz_state_compress), "qlz_state_compress");
	if (!handler->state_compress) return NULL;
    }

    // load key_info for every key
    DBC *cursor;
    if ((rc = handler->dbKeys->cursor(handler->dbKeys, handler->txn, &cursor, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error in cursor: %s", db_strerror(rc));
	return NULL;
    }

    while (1) {
	DBT dbkey, dbdata;

	memset(&dbkey, 0, sizeof(dbkey));
	memset(&dbdata, 0, sizeof(dbdata));
	if ((rc = cursor->get(cursor, &dbkey, &dbdata, DB_NEXT)) != 0) {
	    if (rc == DB_NOTFOUND)
		break;
	    dbats_log(LOG_ERROR, "Error in cursor->get: %s", db_strerror(rc));
	    return NULL;
	}
	raw_key_info_t *rki = dbdata.data;
	char *keycopy = emalloc(dbkey.size+1, "copy of key");
	if (!keycopy) return NULL;
	memcpy(keycopy, dbkey.data, dbkey.size);
	keycopy[dbkey.size] = 0; // nul-terminate
	dbats_key_info_t *dkip = new_key(handler, rki->key_id, keycopy);
	if (!dkip) return NULL;
    }

    if ((rc = cursor->close(cursor)) != 0) {
	dbats_log(LOG_ERROR, "Error in cursor->close: %s", db_strerror(rc));
	return NULL;
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

    if (handler->tslice[0]->time > 0) {
	// XXX fix this?
	dbats_log(LOG_ERROR,
	    "Adding a new aggregation to existing data is not yet supported.");
	return -1;
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
    dbats_log(LOG_INFO, "Flush t=%u agg_id=%d", handler->tslice[agg_id]->time, agg_id);
    size_t frag_size = fragsize(handler);
    fragkey_t dbkey;
    char *buf = NULL;
    int rc;

    // Write fragments to the DB
    dbkey.time = handler->tslice[agg_id]->time;
    dbkey.agg_id = agg_id;
    for (int f = 0; f < handler->tslice[agg_id]->num_frags; f++) {
	if (!handler->tslice[agg_id]->frag[f]) continue;
	dbkey.frag_id = f;
	if (handler->cfg.readonly) {
	    // do nothing
	} else if (!handler->tslice[agg_id]->frag_changed[f]) {
	    dbats_log(LOG_INFO, "Skipping frag %d (unchanged)", f);
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
		dbats_log(LOG_INFO, "Frag %d compression: %zu -> %zu (%.1f %%)",
		    f, frag_size, compressed_len, compressed_len*100.0/frag_size);
		rc = raw_db_set(handler, handler->dbData, &dbkey, sizeof(dbkey),
		    buf, compressed_len + 1);
		if (rc != 0) goto abort;
		handler->tslice[agg_id]->frag_changed[f] = 0;
	    } else {
		if (!buf)
		    buf = (char*)emalloc(frag_size + 1, "write buffer");
		handler->tslice[agg_id]->frag[f]->compressed = 0;
		dbats_log(LOG_INFO, "Frag %d write: %zu", f, frag_size);
		rc = raw_db_set(handler, handler->dbData, &dbkey, sizeof(dbkey),
		    handler->tslice[agg_id]->frag[f], frag_size);
		if (rc != 0) goto abort;
		handler->tslice[agg_id]->frag_changed[f] = 0;
	    }

	    rc = raw_db_set(handler, handler->dbIsSet, &dbkey, sizeof(dbkey),
		handler->tslice[agg_id]->is_set[f], vec_size(ENTRIES_PER_FRAG));
	    if (rc != 0) goto abort;
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

    if (!handler->cfg.readonly) {
	int changed = 0;
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
	    char keybuf[32];
	    sprintf(keybuf, "agg%d", agg_id);
	    rc = raw_db_set(handler, handler->dbMeta, keybuf, strlen(keybuf),
		&handler->agg[agg_id], sizeof(dbats_agg));
	    if (rc != 0) goto abort;
	}
    }

    if (!handler->cfg.exclusive) {
	assert(!handler->tslice[agg_id]->is_set[0]);
	memset(handler->tslice[agg_id], 0, sizeof(dbats_tslice));
    }
    if (buf) free(buf);
    return 0;

abort:
    abort_transaction(handler);
    return rc;
}

/*************************************************************************/

static int dbats_commit(dbats_handler *handler)
{
    if (!handler->cfg.no_txn && !handler->txn) return 0;

    dbats_log(LOG_INFO, "commit %u", handler->tslice[0]->time);

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
	// commit the outer transaction used to emulate exclusive locking
	handler->txn = handler->outer_txn;
	handler->outer_txn = NULL;
	commit_transaction(handler);
    }

    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	free(handler->tslice[agg_id]);
	handler->tslice[agg_id] = NULL;
    }

    for (uint32_t key_id = 0; key_id < handler->cfg.num_keys; key_id++) {
	dbats_key_info_t *dkip = find_key(handler, key_id);
	free((void*)dkip->key);
	dkip->key = NULL;
    }
    for (uint32_t block = 0;
	block < (handler->cfg.num_keys + KEYINFO_PER_BLK - 1) / KEYINFO_PER_BLK;
	block++)
    {
	free(handler->key_info_block[block]);
	handler->key_info_block[block] = NULL;
    }

    if (handler->db_get_buf) free(handler->db_get_buf);
    if (handler->agg) free(handler->agg);
    if (handler->tslice) free(handler->tslice);
    if (handler->key_info_block) free(handler->key_info_block);

    //handler->dbMeta->close(handler->dbMeta, 0);
    //handler->dbKeys->close(handler->dbKeys, 0);
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
    int rc;
    size_t value_len = vec_size(ENTRIES_PER_FRAG);

    *dest = emalloc(vec_size(ENTRIES_PER_FRAG), "is_set");
    if (!*dest)
	return errno ? errno : ENOMEM; // error

    rc = raw_db_get(handler, handler->dbIsSet, dbkey, sizeof(*dbkey),
	*dest, &value_len, !handler->cfg.readonly);
    // assert(value_len == vec_size(ENTRIES_PER_FRAG));
    dbats_log(LOG_INFO, "load_frag t=%u, agg_id=%u, frag_id=%u: "
	"raw_db_get(is_set) = %d", dbkey->time, dbkey->agg_id, dbkey->frag_id, rc);
    if (rc != 0) {
	memset(*dest, 0, vec_size(ENTRIES_PER_FRAG));
	if (rc == DB_NOTFOUND) return 0; // no match
	abort_transaction(handler);
	return rc; // error
    }
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
    if (load_isset(handler, &dbkey, &handler->tslice[agg_id]->is_set[frag_id]) != 0)
	return 0; // no match

    if (handler->db_get_buf_len < fragsize(handler) + QLZ_OVERHEAD) {
	if (handler->db_get_buf) free(handler->db_get_buf);
	handler->db_get_buf_len = fragsize(handler) + QLZ_OVERHEAD;
	handler->db_get_buf = emalloc(handler->db_get_buf_len, "get buffer");
	if (!handler->db_get_buf) return errno ? errno : ENOMEM;
    }

    size_t value_len = handler->db_get_buf_len;
    rc = raw_db_get(handler, handler->dbData, &dbkey, sizeof(dbkey),
	handler->db_get_buf, &value_len, !handler->cfg.readonly);
    dbats_log(LOG_INFO, "load_frag t=%u, agg_id=%u, frag_id=%u: "
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
	dbats_log(LOG_INFO, "decompressed frag t=%u, agg_id=%u, frag_id=%u: "
	    "%u -> %u (%.1f%%)",
	    t, agg_id, frag_id, value_len, len, len*100.0/value_len);

    } else {
	// copy fragment
	// XXX TODO: don't memcpy; get directly into ptr
	memcpy(ptr, handler->db_get_buf, len);
	dbats_log(LOG_INFO, "copied frag t=%u, agg_id=%u, frag_id=%u",
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
	    load_isset(handler, &dbkey, &handler->is_set[ti][frag_id]); // XXX error check
	    if (t == handler->tslice[0]->time) {
		assert(!handler->tslice[0]->is_set[frag_id]);
		handler->tslice[0]->is_set[frag_id] = handler->is_set[ti][frag_id]; // share
	    }
	}
    }
    return 0;
}

int dbats_select_time(dbats_handler *handler, uint32_t time_value, uint32_t flags)
{
    int rc;

    dbats_log(LOG_INFO, "select_time %u", time_value);

    if ((rc = dbats_commit(handler)) != 0)
	return rc;

    if ((rc = begin_transaction(handler, "select time")) != 0)
	return rc;

    handler->preload = !!(flags & DBATS_PRELOAD);

    uint32_t min_time = 0;
    uint32_t max_time = 0;

    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	dbats_tslice *tslice = handler->tslice[agg_id];
	uint32_t t = time_value;

	dbats_normalize_time(handler, agg_id, &t);

	if (handler->cfg.exclusive) {
	    if (tslice->time == t) {
		dbats_log(LOG_INFO, "select_time %u, agg_id=%d: already loaded", t, agg_id);
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

	if (!min_time || t < min_time)
	    min_time = t;
	if (t < handler->agg[0].times.end) {
	    t = min(handler->agg[0].times.end,
		t + handler->agg[agg_id].period - handler->agg[0].period);
	}
	if (t > max_time)
	    max_time = t;
    }
    handler->active_start = min_time;
    handler->active_end = max_time;

    if (!handler->preload)
	return 0;

    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	dbats_tslice *tslice = handler->tslice[agg_id];
	uint32_t t = tslice->time;

	int loaded = 0;
	tslice->num_frags = (handler->cfg.num_keys + ENTRIES_PER_FRAG - 1) /
	    ENTRIES_PER_FRAG;
	for (uint32_t frag_id = 0; frag_id < tslice->num_frags; frag_id++) {
	    if ((rc = load_frag(handler, t, agg_id, frag_id)) != 0)
		return rc;
	    if (tslice->frag[frag_id])
		loaded++;
	}
	dbats_log(LOG_INFO, "select_time %u: agg_id=%d, loaded %u/%u fragments",
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
    raw_key_info_t rki;
    size_t len = sizeof(rki);
    int rc;

    rc = raw_db_get(handler, handler->dbKeys, key, keylen, &rki, &len, 0);
    if (rc == 0) {
	// found it
	*key_id_p = rki.key_id;
	dbats_log(LOG_INFO, "Found key #%u: %s", *key_id_p, key);
	return 0;

    } else if (rc != DB_NOTFOUND) {
	// error
	goto abort;

    } else if (!(flags & DBATS_CREATE)) {
	// didn't find, and can't create
	dbats_log(LOG_INFO, "Key not found: %s", key);
	return rc;

    } else if (handler->cfg.num_keys >= MAX_NUM_KEYINFOBLK * KEYINFO_PER_BLK) {
	// out of space
	dbats_log(LOG_ERROR, "Out of space for key %s", key);
	return rc;

    } else {
	// create
	*key_id_p = handler->cfg.num_keys++;
	dbats_key_info_t *dkip = new_key(handler, *key_id_p, strdup(key));
	if (!dkip) return errno ? errno : ENOMEM;
	rki.key_id = dkip->key_id;
	rc = raw_db_set(handler, handler->dbKeys, dkip->key, strlen(dkip->key), &rki, sizeof(rki));
	if (rc != 0) goto abort;
	dbats_log(LOG_INFO, "Assigned key #%u: %s", *key_id_p, key);
	rc = raw_db_set(handler, handler->dbMeta,
	    "num_keys", strlen("num_keys"),
	    &handler->cfg.num_keys, sizeof(handler->cfg.num_keys));
	if (rc != 0) goto abort;
	return 0;
    }

abort:
    abort_transaction(handler);
    return rc;
}

const char *dbats_get_key_name(dbats_handler *handler, uint32_t key_id)
{
    if (key_id >= handler->cfg.num_keys) return NULL;
    dbats_key_info_t *dkip = find_key(handler, key_id);
    return dkip ? dkip->key : NULL;
}

/*************************************************************************/

static int instantiate_frag_func(dbats_handler *handler, int agg_id, uint32_t frag_id)
{
    dbats_tslice *tslice = handler->tslice[agg_id];

    if (!handler->preload || handler->cfg.readonly) {
	// Try to load the fragment.
	int rc = load_frag(handler, tslice->time, agg_id, frag_id);
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
    dbats_log(LOG_INFO, "grow tslice for time %u, agg_id %d, frag_id %u",
	tslice->time, agg_id, frag_id);

    size_t len = fragsize(handler);
    if (!(tslice->frag[frag_id] = ecalloc(1, len, "tslice->frag[frag_id]"))) {
	return errno ? errno : ENOMEM;
    }

    assert(tslice->is_set[frag_id]);

    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;
    if (agg_id == 0) {
	instantiate_isset_frags(handler, frag_id);
    }

    dbats_log(LOG_INFO, "Grew tslice to %u elements",
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
    dbats_log(LOG_INFO, "dbats_set %u #%u = %" PRIval, handler->tslice[0]->time, key_id, valuep[0]); // XXX
    int rc;

    dbats_key_info_t *dkip = find_key(handler, key_id);

    if (!handler->is_open || handler->cfg.readonly) {
	dbats_log(LOG_ERROR, "is_open=%d, readonly=%d", handler->is_open, handler->cfg.readonly);
	return -1;
    }
    if ((rc = instantiate_frag(handler, 0, dkip->frag_id)) != 0) {
	abort_transaction(handler);
	handler->cfg.readonly = 1;
	return rc;
    }

    uint8_t was_set = vec_test(handler->tslice[0]->is_set[dkip->frag_id], dkip->offset);
    dbats_value *oldvaluep = valueptr(handler, 0, dkip->frag_id, dkip->offset);

    // For each agg_id, aggregate *valuep into tslice[agg_id].
    for (int agg_id = 1; agg_id < handler->cfg.num_aggs; agg_id++) {
	if ((rc = instantiate_frag(handler, agg_id, dkip->frag_id)) != 0) {
	    abort_transaction(handler);
	    handler->cfg.readonly = 1;
	    return rc;
	}

	uint8_t changed = 0;
	uint8_t failed = 0;
	dbats_value *aggval = valueptr(handler, agg_id, dkip->frag_id, dkip->offset);

	//uint32_t aggstart = handler->tslice[agg_id]->time;
	//uint32_t aggend = aggstart + (handler->agg[agg_id].steps - 1) * handler->agg[0].period;

	// Count the number of steps contributing to the aggregate.
	int n = 0;
	{
	    uint32_t t = handler->tslice[agg_id]->time;
	    int ti = (t - handler->active_start) / handler->agg[0].period;
	    while (t <= handler->active_end &&
		t < handler->tslice[agg_id]->time + handler->agg[agg_id].period)
	    {
		if (handler->tslice[0]->time == t ||
		    vec_test(handler->is_set[ti][dkip->frag_id], dkip->offset))
			n++;
		ti++;
		t += handler->agg[0].period;
	    }
	}

	dbats_log(LOG_INFO, "agg %d: aggval=%" PRIval " n=%d",
	    agg_id, aggval[0], n); // XXX

	switch (handler->agg[agg_id].func) {
	case DBATS_AGG_MIN:
	    for (int i = 0; i < handler->cfg.values_per_entry; i++) {
		if (was_set && valuep[i] == oldvaluep[i]) {
		    // value did not change; no need to change agg value
		} else if (n == 1 || valuep[i] <= aggval[i]) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else if (was_set && oldvaluep[i] == aggval[i]) {
		    // XXX TODO: Find the min value among all the steps.
		    failed = ENOSYS; // XXX
		}
	    }
	    break;
	case DBATS_AGG_MAX:
	    for (int i = 0; i < handler->cfg.values_per_entry; i++) {
		if (was_set && valuep[i] == oldvaluep[i]) {
		    // value did not change; no need to change agg value
		} else if (n == 1 || valuep[i] >= aggval[i]) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else if (was_set && oldvaluep[i] == aggval[i]) {
		    // XXX TODO: Find the max value among all the steps.
		    failed = ENOSYS; // XXX
		}
	    }
	    break;
	case DBATS_AGG_AVG:
	    for (int i = 0; i < handler->cfg.values_per_entry; i++) {
		double *daggval = ((double*)&aggval[i]);
		if (was_set && valuep[i] == oldvaluep[i]) {
		    // value did not change; no need to change agg value
		} else if (n == 1) {
		    *daggval = valuep[i];
		    changed = 1;
		} else if (*daggval != valuep[i]) {
		    if (was_set)
			*daggval -= (oldvaluep[i] - *daggval) / n;
		    *daggval += (valuep[i] - *daggval) / n;
		    changed = 1;
		}
	    }
	    break;
	case DBATS_AGG_LAST:
	    for (int i = 0; i < handler->cfg.values_per_entry; i++) {
		if (was_set && valuep[i] == oldvaluep[i]) {
		    // value did not change; no need to change agg value
		} else if (handler->tslice[0]->time >= handler->agg[0].times.end) {
		    // common case: value is latest ever seen
		    aggval[i] = valuep[i];
		    changed = 1;
		} else {
		    // find time of last sample within this agg period
		    uint32_t t = min(handler->active_end,
			handler->tslice[agg_id]->time + handler->agg[agg_id].period - handler->agg[0].period);
		    int ti = (t - handler->active_start) / handler->agg[0].period;
		    while (t >= handler->tslice[agg_id]->time) {
			if (t == handler->tslice[0]->time) {
			    // the new value is the last
			    aggval[i] = valuep[i];
			    changed = 1;
			    break;
			}
			if (vec_test(handler->is_set[ti][dkip->frag_id], dkip->offset))
			    // an existing value is later than the new value
			    break;
			t -= handler->agg[0].period;
			ti--;
		    }
		}
	    }
	    break;
	case DBATS_AGG_SUM:
	    for (int i = 0; i < handler->cfg.values_per_entry; i++) {
		if (was_set && valuep[i] == oldvaluep[i]) {
		    // value did not change; no need to change agg value
		} else if (n == 1) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else {
		    if (was_set)
			aggval[i] -= oldvaluep[i];
		    aggval[i] += valuep[i];
		    changed = 1;
		}
	    }
	    break;
	}

	if (changed || failed) {
	    int level = failed ? LOG_ERROR : LOG_INFO;
	    if (level <= dbats_log_level) { 
		char aggbuf[64];
		if (handler->agg[agg_id].func == DBATS_AGG_AVG)
		    sprintf(aggbuf, "%f", ((double*)aggval)[0]);
		else
		    sprintf(aggbuf, "%" PRIval, aggval[0]);
		dbats_log(level, "%s set value agg_id=%d frag_id=%u "
		    "offset=%" PRIu32 " value_len=%u aggval=%s",
		    failed ? "Failed to" : "Successfully",
		    agg_id, dkip->frag_id, dkip->offset, handler->cfg.entry_size, aggbuf);
	    }
	}
	if (changed) {
	    // XXX if (n >= xff * steps)
	    vec_set(handler->tslice[agg_id]->is_set[dkip->frag_id], dkip->offset);
	    handler->tslice[agg_id]->frag_changed[dkip->frag_id] = 1;
	} else if (failed) {
	    handler->cfg.readonly = 1;
	    return failed;
	}
    }

    // Set value at agg_id 0 (after aggregations because aggregations need
    // both old and new values)
    memcpy(oldvaluep, valuep, handler->cfg.entry_size);
    vec_set(handler->tslice[0]->is_set[dkip->frag_id], dkip->offset);
    handler->tslice[0]->frag_changed[dkip->frag_id] = 1;
    dbats_log(LOG_INFO, "Succesfully set value "
	"agg_id=%d frag_id=%u offset=%" PRIu32 " value_len=%u value=%" PRIval,
	0, dkip->frag_id, dkip->offset, handler->cfg.entry_size, valuep[0]);

    return 0;
}

int dbats_set_by_key(dbats_handler *handler, const char *key,
    const dbats_value *valuep)
{
    int rc;
    uint32_t key_id;

    if ((rc = dbats_get_key_id(handler, key, &key_id, DBATS_CREATE)) != 0) {
	handler->cfg.readonly = 1;
	return rc;
    }
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

    dbats_key_info_t *dkip = find_key(handler, key_id);

    if ((rc = instantiate_frag(handler, agg_id, dkip->frag_id)) == 0) {
	if (!vec_test(tslice->is_set[dkip->frag_id], dkip->offset)) {
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, agg_id, dkip->key);
	    *valuepp = NULL;
	    return 666;
	}
	if (handler->agg[agg_id].func == DBATS_AGG_AVG) {
	    double *dval = (double*)valueptr(handler, agg_id, dkip->frag_id, dkip->offset);
	    static dbats_value *avg_buf = NULL;
	    if (!avg_buf) {
		avg_buf = emalloc(handler->cfg.entry_size * handler->cfg.values_per_entry, "avg_buf");
		if (!avg_buf) return errno ? errno : ENOMEM;
	    }
	    for (int i = 0; i < handler->cfg.values_per_entry; i++)
		avg_buf[i] = dval[i] + 0.5;
	    *valuepp = avg_buf;
	} else {
	    *valuepp = valueptr(handler, agg_id, dkip->frag_id, dkip->offset);
	}
	dbats_log(LOG_INFO, "Succesfully read value [offset=%" PRIu32 "][value_len=%u]",
	    dkip->offset, handler->cfg.entry_size);

    } else {
	if (rc == DB_NOTFOUND)
	    dbats_log(LOG_WARNING, "Value unset (f): %u %d %s",
		tslice->time, agg_id, dkip->key);
	*valuepp = NULL;
	return rc;
    }

    return 0;
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

    dbats_key_info_t *dkip = find_key(handler, key_id);

    if ((rc = instantiate_frag(handler, agg_id, dkip->frag_id)) == 0) {
	if (!vec_test(tslice->is_set[dkip->frag_id], dkip->offset)) {
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, agg_id, dkip->key);
	    *valuepp = NULL;
	    return 1;
	}
	*valuepp = (double*)valueptr(handler, agg_id, dkip->frag_id, dkip->offset);
	dbats_log(LOG_INFO, "Succesfully read value [offset=%" PRIu32 "][value_len=%u]",
	    dkip->offset, handler->cfg.entry_size);

    } else {
	if (rc == DB_NOTFOUND)
	    dbats_log(LOG_WARNING, "Value unset (f): %u %d %s",
		tslice->time, agg_id, dkip->key);
	*valuepp = NULL;
	return rc;
    }

    return 0;
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

    if ((rc = handler->dbKeys->cursor(handler->dbKeys, handler->txn, &handler->keyname_cursor, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error in dbats_walk_keyname_start: %s", db_strerror(rc));
	return -1;
    }
    return 0;
}

int dbats_walk_keyname_next(dbats_handler *handler, uint32_t *key_id_p)
{
    int rc;
    DBT dbkey, dbdata;

    memset(&dbkey, 0, sizeof(dbkey));
    memset(&dbdata, 0, sizeof(dbdata));
    if ((rc = handler->keyname_cursor->get(handler->keyname_cursor, &dbkey, &dbdata, DB_NEXT)) != 0) {
	if (rc != DB_NOTFOUND)
	    dbats_log(LOG_ERROR, "Error in dbats_walk_keyname_next: %s", db_strerror(rc));
	return -1;
    }
    *key_id_p = ((raw_key_info_t*)dbdata.data)->key_id;
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
    handler->keyid_walker = 0;
    return 0;
}

int dbats_walk_keyid_next(dbats_handler *handler, uint32_t *key_id_p)
{
    dbats_key_info_t *dkip;
    while (handler->keyid_walker < handler->cfg.num_keys) {
	dkip = find_key(handler, handler->keyid_walker++);
	if (dkip->key) {
	    *key_id_p = dkip->key_id;
	    return 0;
	}
    }
    return -1;
}

int dbats_walk_keyid_end(dbats_handler *handler)
{
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
    if ((rc = handler->dbKeys->stat_print(handler->dbKeys, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "Error while dumping DB stats for Key [%s]", db_strerror(rc));
    if ((rc = handler->dbData->stat_print(handler->dbData, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "Error while dumping DB stats for Data [%s]", db_strerror(rc));
    if ((rc = handler->dbIsSet->stat_print(handler->dbIsSet, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "Error while dumping DB stats for IsSet [%s]", db_strerror(rc));
}

