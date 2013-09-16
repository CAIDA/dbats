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
#include "config.h"

// Limits
#define ENTRIES_PER_FRAG    10000 // number of entries in a fragment
#define MAX_NUM_FRAGS       16384 // max number of fragments in a tslice
#define MAX_NUM_BUNDLES        16 // max number of time series bundles

#define TXN_PER_OP 0 // must be 0, because rolling back a dbats_set doesn't reset the in-memory fragments

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

#define valueptr(dh, bid, frag_id, offset) \
    ((dbats_value*)(&dh->tslice[bid]->frag[frag_id]->data[offset * dh->cfg.entry_size]))

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
    uint32_t bid;
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
    uint8_t changed;           // has changes that need to be written to db?
    uint8_t emulate_exclusive; // emulating set_lk_exclusive()?
    void *state_compress;
    void *state_decompress;
    DB_ENV *dbenv;             // DB environment
    DB *dbMeta;                // config parameters
    DB *dbBundle;              // bundle parameters
    DB *dbKeyname;             // key name -> key id
    DB *dbKeyid;               // recno -> key name (note: keyid == recno - 1)
    DB *dbData;                // {bid, time, frag_id} -> value fragment
    DB *dbIsSet;               // {bid, time, frag_id} -> is_set fragment
    DBC *keyname_cursor;       // for iterating over key names
    DBC *keyid_cursor;         // for iterating over key ids
    dbats_tslice **tslice;     // a tslice for each time series bundle
    dbats_bundle_info *bundle; // parameters for each time series bundle
    uint8_t *db_get_buf;       // buffer for data fragment
    size_t db_get_buf_len;
    DB_TXN *txn[N_TXN];        // stack of transactions ([0] is NULL)
    int n_txn;                 // number of transactions in stack (not incl [0])
    uint8_t action_in_prog;    // action in progress?
    uint8_t action_cancelled;  // cancelled due to deadlock
    uint32_t active_start;     // first timestamp in active period
                               // (corresponding to is_set[0])
    uint32_t active_end;       // last timestamp in active period
    uint32_t active_last_data; // last timestamp in active period with data
    uint32_t end_time;         // latest time for which a value has been set
    uint32_t min_keep_time;    // earliest time for which a value can be set
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
    memset(&_var, 0, sizeof(DBT)); \
    _var.flags = DB_DBT_PARTIAL

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
    rc = dh->dbenv->txn_begin(dh->dbenv, parent, childp, 0);
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
#if HAVE_DB_SET_LK_EXCLUSIVE
	if ((rc = (*dbp)->set_lk_exclusive(*dbp, 1)) != 0) {
	    dbats_log(LOG_ERROR, "Error obtaining exclusive lock on %s/%s: %s",
		dh->path, name, db_strerror(rc));
	    return rc;
	}
#endif
    }

    uint32_t dbflags = 0;
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

static void log_db_op(int level, const char *fname, int line, dbats_handler *dh, DB *db,
    const char *label, const void *key, uint32_t key_len, const char *msg)
{
    char keybuf[DBATS_KEYLEN] = "";
    if (db == dh->dbData || db == dh->dbIsSet) {
	fragkey_t *fragkey = (fragkey_t*)key;
	sprintf(keybuf, "bid=%d t=%u frag=%u",
	    ntohl(fragkey->bid), ntohl(fragkey->time), ntohl(fragkey->frag_id));
    } else if (db == dh->dbBundle) {
	sprintf(keybuf, "%d", *(uint8_t*)key);
    } else if (db == dh->dbMeta || db == dh->dbKeyname) {
	sprintf(keybuf, "\"%.*s\"", key_len, (char*)key);
    } else if (db == dh->dbKeyid) {
	sprintf(keybuf, "#%u", *(db_recno_t*)key);
    }
    const char *dbname;
    db->get_dbname(db, &dbname, NULL);
    dbats_log_func(level, fname, line, "%s: %s: %s: %s",
	label, dbname, keybuf, msg);
}

#define raw_db_set(dh, db, key, key_len, value, value_len) \
    (raw_db_set)(__FILE__, __LINE__, dh, db, key, key_len, value, value_len)

static int (raw_db_set)(
    const char *fname, int line,
    dbats_handler *dh, DB *db,
    const void *key, uint32_t key_len,
    const void *value, uint32_t value_len)
{
    log_db_op(LOG_FINEST, fname, line, dh, db, "db_set", key, key_len, "");
    int rc;

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
	if (dh->action_in_prog && rc == DB_LOCK_DEADLOCK)
	    dh->action_cancelled = 1;
	log_db_op(LOG_WARNING, fname, line, dh, db, "db_set", key, key_len, db_strerror(rc));
	return rc;
    }
    return 0;
}

#define QLZ_OVERHEAD 400

#define raw_db_get(dh, db, key, key_len, value, buflen, value_len_p, dbflags) \
    (raw_db_get)(__FILE__, __LINE__, dh, db, key, key_len, value, buflen, value_len_p, dbflags)

// Get a value from the database.
// buflen must contain the length of the memory pointed to by value.
// If value_len_p != NULL, then after the call, *value_len_p will contain the
// length of data written into the memory pointed to by value.
static int (raw_db_get)(const char *fname, int line,
    dbats_handler *dh, DB* db,
    const void *key, uint32_t key_len,
    void *value, size_t buflen, uint32_t *value_len_p,
    uint32_t dbflags)
{
    const char *label =
	(dbflags & DB_RMW) ? "db_get(rw)" :
	(dbflags & DB_READ_COMMITTED) ? "db_get(t)" :
	"db_get(r)";
    log_db_op(LOG_FINEST, fname, line, dh, db, label, key, key_len, "");
    int rc;

    DBT_in(dbt_key, (void*)key, key_len);
    DBT_out(dbt_data, value, buflen);
    rc = db->get(db, current_txn(dh), &dbt_key, &dbt_data, dbflags);
    if (rc == 0) {
	if (value_len_p) {
	    *value_len_p = dbt_data.size;
	} else if (dbt_data.size != buflen) {
	    dbats_log(LOG_WARNING, "db_get: expected %" PRIu32 ", got %" PRIu32,
		dbt_data.ulen, dbt_data.size);
	}
	return 0;
    }
    log_db_op(rc == DB_NOTFOUND ? LOG_FINE : LOG_WARNING, fname, line, dh, db,
	label, key, key_len, db_strerror(rc));
    if (dh->action_in_prog && rc == DB_LOCK_DEADLOCK)
	dh->action_cancelled = 1;
    if (rc == DB_BUFFER_SMALL) {
	dbats_log(LOG_WARNING, "db_get: had %" PRIu32 ", needed %" PRIu32,
	    dbt_data.ulen, dbt_data.size);
    }
    return rc;
}

#define raw_cursor_get(dh, db, dbc, key, data, flags) \
    (raw_cursor_get)(__FILE__, __LINE__, dh, db, dbc, key, data, flags)

static int (raw_cursor_get)(const char *fname, int line,
    dbats_handler *dh, DB *db, DBC *dbc, DBT *key, DBT *data, int flags)
{
#if (DB_VERSION_MAJOR < 5)
    // version 4 didn't support DB_READ_COMMITTED in cursor get
    flags = flags & ~DB_READ_COMMITTED;
#endif
    if (dbats_log_level >= LOG_FINEST) {
	char label[32];
	int op = flags & DB_OPFLAGS_MASK;
	const char *opstr =
	    (op == DB_NEXT) ? "NEXT" :
	    (op == DB_PREV) ? "PREV" :
	    (op == DB_FIRST) ? "FIRST" :
	    (op == DB_LAST) ? "LAST" :
	    (op == DB_SET_RANGE) ? "SET_RANGE" :
	    "?";
	const char *lockstr =
	    (flags & DB_RMW) ? "rw" :
	    (flags & DB_READ_COMMITTED) ? "t" :
	    "r";
	sprintf(label, "cursor_get(%s,%s)", opstr, lockstr);
	log_db_op(LOG_FINEST, fname, line, dh, db, label, key->data, key->size, "");
    }
    int rc = dbc->get(dbc, key, data, flags);
    if (dh->action_in_prog && rc == DB_LOCK_DEADLOCK)
	dh->action_cancelled = 1;
    return rc;
}

#define CFG_SET(dh, key, val) \
    raw_db_set(dh, dh->dbMeta, key, strlen(key), &val, sizeof(val))

#define CFG_GET(dh, key, val, flags) \
    raw_db_get(dh, dh->dbMeta, key, strlen(key), &val, sizeof(val), NULL, flags)

/*************************************************************************/

#define bundle_info_key(dh, keybuf, bid) \
    sprintf(keybuf, dh->cfg.version >= 4 ? "bundle%d" : \
	dh->cfg.version >= 3 ? "series%d" : "agg%d", bid)

static int read_bundle_info(dbats_handler *dh, int bid, uint32_t flags)
{
    if (dh->cfg.version >= 5) {
	uint8_t key = bid;
	return raw_db_get(dh, dh->dbBundle, &key, sizeof(key), &dh->bundle[bid],
	    sizeof(dh->bundle[bid]), NULL, flags);
    } else {
	char keybuf[32];
	bundle_info_key(dh, keybuf, bid);
	return CFG_GET(dh, keybuf, dh->bundle[bid], flags);
    }
}

static int write_bundle_info(dbats_handler *dh, int bid)
{
    if (dh->cfg.version >= 5) {
	uint8_t key = bid;
	return raw_db_set(dh, dh->dbBundle, &key, sizeof(key), &dh->bundle[bid],
	    sizeof(dh->bundle[bid]));
    } else {
	char keybuf[32];
	bundle_info_key(dh, keybuf, bid);
	return CFG_SET(dh, keybuf, dh->bundle[bid]);
    }
}

/*************************************************************************/

// Warning: never open the same dbats more than once in the same process,
// because there must be only one DB_ENV per environment per process.
// http://docs.oracle.com/cd/E17076_02/html/programmer_reference/transapp_app.html
dbats_handler *dbats_open(const char *path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags)
{
    dbats_log(LOG_CONFIG, "db_version: '%s'", db_version(0,0,0));

    int rc;
    int mode = 00666;
    uint32_t dbflags = DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG |
	DB_INIT_TXN | DB_REGISTER | DB_RECOVER | DB_CREATE;
    int is_new = 0;

    dbats_handler *dh = ecalloc(1, sizeof(dbats_handler), "dh");
    if (!dh) return NULL;
    dh->cfg.readonly = !!(flags & DBATS_READONLY);
    dh->cfg.updatable = !!(flags & DBATS_UPDATABLE);
    dh->cfg.compress = !(flags & DBATS_UNCOMPRESSED);
    dh->cfg.exclusive = !!(flags & DBATS_EXCLUSIVE);
    dh->cfg.no_txn = !!(flags & DBATS_NO_TXN);
    dh->cfg.values_per_entry = 1;
    dh->serialize = !(flags & DBATS_MULTIWRITE);
    dh->path = path;

    if ((rc = db_env_create(&dh->dbenv, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error creating DB env: %s",
	    db_strerror(rc));
	return NULL;
    }

    dh->dbenv->set_errpfx(dh->dbenv, dh->path);
    dh->dbenv->set_errfile(dh->dbenv, stderr);

    dh->dbenv->set_verbose(dh->dbenv,
	DB_VERB_FILEOPS | DB_VERB_RECOVERY | DB_VERB_REGISTER, 1);
    dh->dbenv->set_msgfile(dh->dbenv, stderr);

    if ((flags & DBATS_NO_TXN) && (flags & DBATS_EXCLUSIVE)) {
	dbats_log(LOG_ERROR,
	    "DBATS_EXCLUSIVE and DBATS_NO_TXN are not compatible");
	return NULL;
    }

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
	    int tries_limit = 30;
	    sprintf(filename, "%s/meta", path);
	    while (stat(filename, &statbuf) != 0) {
		if (errno != ENOENT) {
		    dbats_log(LOG_ERROR, "Error checking %s: %s",
			filename, strerror(errno));
		    return NULL;
		}
		if (--tries_limit <= 0) {
		    dbats_log(LOG_ERROR, "Retry limit exceeded waiting for %s",
			filename);
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

	// XXX These should be based on max expected number of metric keys?
	// 40000 is enough to insert at least 2000000 metric keys.
	fprintf(file, "set_lk_max_locks 40000\n");
	fprintf(file, "set_lk_max_objects 40000\n");

	fprintf(file, "set_lk_detect DB_LOCK_YOUNGEST\n");
	fprintf(file, "set_lg_bsize 1048576\n"); // default is ~32K

	fclose(file);
    }

    if ((rc = dh->dbenv->open(dh->dbenv, path, dbflags, mode)) != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s: %s", path, db_strerror(rc));
	return NULL;
    }

#if defined(DB_LOG_AUTO_REMOVE)
    dh->dbenv->log_set_config(dh->dbenv, DB_LOG_AUTO_REMOVE, 1);
#elif defined(DB_LOG_AUTOREMOVE)
    dh->dbenv->set_flags(dh->dbenv, DB_LOG_AUTOREMOVE, 1);
#else
# error "no DB_LOG_AUTO_REMOVE option"
#endif

    if (!HAVE_DB_SET_LK_EXCLUSIVE && dh->cfg.exclusive) {
	// emulate exclusive lock with an outer txn and a write lock
	if (begin_transaction(dh, "exclusive txn") != 0) return NULL;
	dh->emulate_exclusive = 1;
    }
    if (begin_transaction(dh, "open txn") != 0) goto abort;
    dh->action_in_prog = 1;
    dh->action_cancelled = 0;

#define open_db(db,name,type) raw_db_open(dh, &dh->db, name, type, flags, mode)
    if (open_db(dbMeta,    "meta",    DB_BTREE) != 0) goto abort;

    dh->cfg.entry_size = sizeof(dbats_value); // for db_get_buf_len

#define initcfg(field, key, defaultval) \
    do { \
	if (is_new) { \
	    dh->cfg.field = (defaultval); \
	    if (CFG_SET(dh, key, dh->cfg.field) != 0) \
		goto abort; \
	} else { \
	    int writelock = !dh->cfg.readonly || dh->emulate_exclusive; \
	    if (CFG_GET(dh, key, dh->cfg.field, writelock ? DB_RMW : 0) != 0) {\
		dbats_log(LOG_ERROR, "%s: missing config: %s", path, key); \
		goto abort; \
	    } \
	} \
	dbats_log(LOG_CONFIG, "cfg: %s = %u", key, dh->cfg.field); \
    } while (0)

    initcfg(version,          "version",           DBATS_DB_VERSION);
    initcfg(period,           "period",            period);
    initcfg(values_per_entry, "values_per_entry",  values_per_entry);

    if (dh->cfg.version > DBATS_DB_VERSION) {
	dbats_log(LOG_ERROR, "database version %d > library version %d",
	    dh->cfg.version, DBATS_DB_VERSION);
	return NULL;
    }

    if (dh->cfg.version >= 5)
	if (open_db(dbBundle,  "bundle",  DB_BTREE) != 0) goto abort;
    if (open_db(dbKeyname, "keyname", DB_BTREE) != 0) goto abort;
    if (open_db(dbKeyid,   "keyid",   DB_RECNO) != 0) goto abort;
    if (open_db(dbData,    "data",    DB_BTREE) != 0) goto abort;
    if (open_db(dbIsSet,   "is_set",  DB_BTREE) != 0) goto abort;
#undef open_db
#undef initcfg

    dh->bundle = emalloc(MAX_NUM_BUNDLES * sizeof(*dh->bundle), "dh->bundle");
    if (!dh->bundle) return NULL;
    dh->tslice = emalloc(MAX_NUM_BUNDLES * sizeof(*dh->tslice), "dh->tslice");
    if (!dh->tslice) return NULL;

    dh->cfg.entry_size = dh->cfg.values_per_entry * sizeof(dbats_value);

    // load metadata for time series bundles
    if (is_new) {
	memset(&dh->bundle[0], 0, sizeof(dbats_bundle_info));
	dh->bundle[0].func = DBATS_AGG_NONE;
	dh->bundle[0].steps = 1;
	dh->bundle[0].period = dh->cfg.period;
	dh->cfg.num_bundles = 1;
	rc = write_bundle_info(dh, 0);
	if (rc != 0) return NULL;
	rc = CFG_SET(dh, "min_keep_time", dh->min_keep_time);
	if (rc != 0) return NULL;
    } else {
	for (int bid = 0; ; bid++) {
	    rc = read_bundle_info(dh, bid, 0);
	    if (rc != 0) {
		if (rc == DB_NOTFOUND && bid > 0) break; // no more bundles
		return NULL;
	    }
	    dh->cfg.num_bundles = bid + 1;
	}
    }

    // allocate tslice for each time series bundle
    for (int bid = 0; bid < dh->cfg.num_bundles; bid++) {
	if (!(dh->tslice[bid] = ecalloc(1, sizeof(dbats_tslice), "tslice")))
	    return NULL;
    }

    if (!dh->cfg.readonly) {
	if (dh->cfg.compress) {
	    dh->state_compress = ecalloc(1, sizeof(qlz_state_compress),
		"qlz_state_compress");
	    if (!dh->state_compress) return NULL;
	}
    }

    dh->is_open = 1;
    return dh;

abort:
    dbats_abort(dh);
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
    }

    if (dh->cfg.num_bundles >= MAX_NUM_BUNDLES) {
	dbats_log(LOG_ERROR, "Too many bundles (%d)", MAX_NUM_BUNDLES);
	return ENOMEM;
    }

    int bid = dh->cfg.num_bundles;

    if (!(dh->tslice[bid] = ecalloc(1, sizeof(dbats_tslice), "tslice")))
	return errno ? errno : ENOMEM;

    memset(&dh->bundle[bid], 0, sizeof(dbats_bundle_info));
    dh->bundle[bid].func = func;
    dh->bundle[bid].steps = steps;
    dh->bundle[bid].period = dh->bundle[0].period * steps;

    rc = write_bundle_info(dh, bid);
    if (rc != 0) return rc;

    dh->cfg.num_bundles++;
    return 0;
}

int dbats_get_start_time(dbats_handler *dh, int bid, uint32_t *start)
{
    int rc;
    DBC *dbc;

    *start = 0;
    rc = dh->dbData->cursor(dh->dbData, current_txn(dh), &dbc, 0);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "dbats_get_start_time %d: cursor: %s", bid, db_strerror(rc));
	return rc;
    }
    fragkey_t dbkey = { htonl(bid), htonl(0), htonl(0) };
    DBT_in(dbt_key, &dbkey, sizeof(dbkey));
    dbt_key.ulen = sizeof(dbkey);
    DBT_null(dbt_data);
    rc = raw_cursor_get(dh, dh->dbData, dbc, &dbt_key, &dbt_data, DB_SET_RANGE | DB_READ_COMMITTED);
    if (rc == DB_NOTFOUND) {
	// do nothing
    } else if (rc != 0) {
	dbats_log(LOG_ERROR, "dbats_get_start_time %d: get: %s", bid, db_strerror(rc));
    } else { // success
	*start = ntohl(dbkey.time);
    }

    dbc->close(dbc);
    return rc;
}

int dbats_get_end_time(dbats_handler *dh, int bid, uint32_t *end)
{
    int rc;
    DBC *dbc;

    *end = 0;
    rc = dh->dbData->cursor(dh->dbData, current_txn(dh), &dbc, 0);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "dbats_get_end_time %d: cursor: %s", bid, db_strerror(rc));
	return rc;
    }
    fragkey_t dbkey = { htonl(bid), htonl(UINT32_MAX), htonl(0) };
    DBT_in(dbt_key, &dbkey, sizeof(dbkey));
    dbt_key.ulen = sizeof(dbkey);
    DBT_null(dbt_data);
    rc = raw_cursor_get(dh, dh->dbData, dbc, &dbt_key, &dbt_data, DB_SET_RANGE | DB_READ_COMMITTED);

    if (rc == 0) {
	rc = raw_cursor_get(dh, dh->dbData, dbc, &dbt_key, &dbt_data, DB_PREV | DB_READ_COMMITTED);
    } else if (rc == DB_NOTFOUND) {
	rc = raw_cursor_get(dh, dh->dbData, dbc, &dbt_key, &dbt_data, DB_LAST | DB_READ_COMMITTED);
    } else {
	dbats_log(LOG_ERROR, "dbats_get_end_time %d: get: %s", bid, db_strerror(rc));
	dbc->close(dbc);
	return rc;
    }

    if (rc == DB_NOTFOUND) {
	// do nothing
    } else if (rc != 0) {
	dbats_log(LOG_ERROR, "dbats_get_end_time %d: prev: %s", bid, db_strerror(rc));
    } else if (ntohl(dbkey.bid) != bid) {
	rc = DB_NOTFOUND;
    } else { // success
	*end = ntohl(dbkey.time);
    }

    dbc->close(dbc);
    return rc;
}

int dbats_best_bundle(dbats_handler *dh, uint32_t func, uint32_t start)
{
    int best_bid = 0;
    uint32_t best_start;
    dbats_get_start_time(dh, 0, &best_start);
    uint32_t best_bounded_start = max(best_start, start);

    for (int bid = 1; bid < dh->cfg.num_bundles; bid++) {
	if (dh->bundle[bid].func != func) continue;
	uint32_t this_start;
	dbats_get_start_time(dh, bid, &this_start);
	uint32_t this_bounded_start = max(this_start, start);
	if (this_bounded_start < best_bounded_start) {
	    best_bid = bid;
	    best_bounded_start = this_bounded_start;
	}
    }
    return best_bid;
}

// "DELETE FROM db WHERE db.bid = bid AND db.time >= start AND db.time < end"
static int delete_frags(dbats_handler *dh, DB *db, int bid,
    uint32_t start, uint32_t end)
{
    const char *name;
    db->get_dbname(db, &name, NULL);

    dbats_log(LOG_FINE, "delete_frags %s %d [%u,%u)", name,
	bid, start, end);
    int rc;
    DBC *dbc;
    rc = db->cursor(db, current_txn(dh), &dbc, 0);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "delete_frags %s %d [%u,%u): cursor: %s", name,
	    bid, start, end, db_strerror(rc));
	return rc;
    }
    fragkey_t dbkey = { htonl(bid), htonl(start), htonl(0) };
    DBT_in(dbt_key, &dbkey, sizeof(dbkey));
    dbt_key.ulen = sizeof(dbkey);
    DBT_null(dbt_data);
    // Range cursor search depends on dbkey being {bid, time, frag_id}, in
    // that order, with big endian values.
    rc = raw_cursor_get(dh, db, dbc, &dbt_key, &dbt_data, DB_SET_RANGE);
    while (1) {
	if (rc == DB_NOTFOUND) break;
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "delete_frags %s %d %u %d: get: %s", name,
		ntohl(dbkey.bid), ntohl(dbkey.time), ntohl(dbkey.frag_id),
		db_strerror(rc));
	    return rc;
	}
	if (ntohl(dbkey.bid) != bid || ntohl(dbkey.time) >= end) break;
	rc = dbc->del(dbc, 0);
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "delete_frags %s %d %u %d: del: %s", name,
		ntohl(dbkey.bid), ntohl(dbkey.time), ntohl(dbkey.frag_id),
		db_strerror(rc));
	    return rc;
	} else {
	    dbats_log(LOG_FINE, "delete_frags %d %u %d",
		ntohl(dbkey.bid), ntohl(dbkey.time), ntohl(dbkey.frag_id));
	}
	rc = raw_cursor_get(dh, db, dbc, &dbt_key, &dbt_data, DB_NEXT);
    }
    rc = dbc->close(dbc);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "delete_frags %s %d [%u,%u): close: %s", name,
	    bid, start, end, db_strerror(rc));
	return rc;
    }
    return 0;
}

// Delete values earlier than the keep limit
static int truncate_bundle(dbats_handler *dh, int bid)
{
    dbats_log(LOG_FINE, "truncate_bundle %d", bid);
    int rc;

    dbats_bundle_info *bundle = &dh->bundle[bid];
    if (bundle->keep == 0) return 0; // keep all

    uint32_t start, end;
    dbats_get_start_time(dh, bid, &start);
    dbats_get_end_time(dh, bid, &end);
    uint32_t keep_period = (bundle->keep-1) * bundle->period;
    uint32_t new_start = (keep_period > end) ? 0 : (end - keep_period);
    if (bid == 0 && dh->min_keep_time < new_start) {
	dh->min_keep_time = new_start;
	rc = CFG_SET(dh, "min_keep_time", dh->min_keep_time);
	if (rc != 0) return rc;
    }
    if (start >= new_start) return 0; // there's nothing to truncate

    rc = begin_transaction(dh, "truncate txn");
    if (rc != 0) return rc;
    rc = delete_frags(dh, dh->dbData, bid, 0, new_start);
    if (rc != 0) goto abort;
    rc = delete_frags(dh, dh->dbIsSet, bid, 0, new_start);
    if (rc != 0) goto abort;
    commit_transaction(dh); // truncate txn
    return 0;

abort:
    // failing to delete frags is just an annoyance, not an error
    abort_transaction(dh); // truncate txn
    return 0;
}

int dbats_series_limit(dbats_handler *dh, int bid, int keep)
{
    int rc = 0;

    rc = begin_transaction(dh, "keep txn");
    if (rc != 0) return rc;

    rc = read_bundle_info(dh, bid, DB_RMW);
    if (rc != 0) goto abort;

    if (bid == 0) {
	for (int a = 1; a < dh->cfg.num_bundles; a++) {
	    rc = read_bundle_info(dh, a, 0);
	    if (rc != 0) goto abort;
	    if (keep > 0 && dh->bundle[a].steps > keep) {
		dbats_log(LOG_ERROR, "bundle %d requires keeping at least %d "
		    "primary data points", a, dh->bundle[a].steps);
		rc = EINVAL;
		goto abort;
	    }
	}
    }

    dh->bundle[bid].keep = keep;
    rc = truncate_bundle(dh, bid);
    if (rc != 0) goto abort;

    rc = write_bundle_info(dh, bid); // write new keep
    if (rc != 0) goto abort;

    return commit_transaction(dh); // "keep txn"

abort:
    abort_transaction(dh); // "keep txn"
    return rc;
}

/*************************************************************************/

// Free contents of in-memory tslice.
static int clear_tslice(dbats_handler *dh, int bid)
{
    dbats_tslice *tslice = dh->tslice[bid];
    dbats_log(LOG_FINE, "Free tslice t=%u bid=%d", tslice->time, bid);

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

// Writes tslice to db.
static int flush_tslice(dbats_handler *dh, int bid)
{
    dbats_log(LOG_FINE, "Flush t=%u bid=%d", dh->tslice[bid]->time, bid);
    size_t frag_size = fragsize(dh);
    char *buf = NULL;
    int rc;

    // Write fragments to the DB
    fragkey_t dbkey = { htonl(bid), htonl(dh->tslice[bid]->time), 0};
    for (int f = 0; f < dh->tslice[bid]->num_frags; f++) {
	if (!dh->tslice[bid]->frag[f]) continue;
	dbkey.frag_id = htonl(f);
	if (!dh->tslice[bid]->frag_changed[f]) {
	    dbats_log(LOG_VERYFINE, "Skipping frag %d (unchanged)", f);
	} else {
	    if (dh->cfg.compress) {
		if (!buf) {
		    buf = (char*)emalloc(1 + frag_size + QLZ_OVERHEAD,
			"compression buffer");
		    if (!buf) return errno ? errno : ENOMEM;
		}
		buf[0] = 1; // compression flag
		dh->tslice[bid]->frag[f]->compressed = 1;
		size_t compress_len = qlz_compress(dh->tslice[bid]->frag[f],
		    buf+1, frag_size, dh->state_compress);
		dbats_log(LOG_VERYFINE, "Frag %d compression: %zu -> %zu "
		    "(%.1f %%)", f, frag_size, compress_len,
		    compress_len*100.0/frag_size);
		rc = raw_db_set(dh, dh->dbData, &dbkey, sizeof(dbkey),
		    buf, compress_len + 1);
		if (rc != 0) return rc;
	    } else {
		dh->tslice[bid]->frag[f]->compressed = 0;
		dbats_log(LOG_VERYFINE, "Frag %d write: %zu", f, frag_size);
		rc = raw_db_set(dh, dh->dbData, &dbkey, sizeof(dbkey),
		    dh->tslice[bid]->frag[f], frag_size);
		if (rc != 0) return rc;
	    }
	    dh->tslice[bid]->frag_changed[f] = 0;

	    rc = raw_db_set(dh, dh->dbIsSet, &dbkey, sizeof(dbkey),
		dh->tslice[bid]->is_set[f], vec_size(ENTRIES_PER_FRAG));
	    if (rc != 0) return rc;
	}
    }
    if (buf) free(buf);

    rc = truncate_bundle(dh, bid);
    if (rc != 0) return rc;

    return 0;
}

/*************************************************************************/

static void free_isset(dbats_handler *dh)
{
    if (dh->is_set) {
	int tn = (dh->active_end - dh->active_start) / dh->bundle[0].period + 1;
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
    int rc = 0;
    if (!dh->action_in_prog) return 0;
    if (dh->action_cancelled) return DB_LOCK_DEADLOCK;
    dbats_log(LOG_FINE, "dbats_commit %u", dh->tslice[0]->time);

    free_isset(dh);

    if (!dh->serialize) {
	// In dbats_select_time(), we read min_keep_time with DB_READ_COMMITTED;
	// it may have changed since then.
	rc = CFG_GET(dh, "min_keep_time", dh->min_keep_time, DB_RMW);
	if (rc != 0) {
	    dbats_log(LOG_ERROR, "error getting %s: %s", "min_keep_time",
		db_strerror(rc));
	    goto end;
	}

	for (int bid = 0; bid < dh->cfg.num_bundles; bid++) {
	    if (dh->changed && dh->tslice[bid]->time < dh->min_keep_time) {
		dbats_log(LOG_ERROR, "dbats_commit %u: illegal attempt to set "
		    "value in bundle %d at time %u before series limit %u",
		    dh->tslice[bid]->time, bid, dh->tslice[bid]->time,
		    dh->min_keep_time);
		rc = EINVAL;
		goto end;
	    }
	}
    }

end:
    for (int bid = 0; bid < dh->cfg.num_bundles; bid++) {
	if (dh->changed && rc == 0) {
	    rc = flush_tslice(dh, bid);
	}
	if (!dh->cfg.exclusive || !dh->is_open || rc != 0) {
	    clear_tslice(dh, bid);
	}
    }

    if (rc == 0)
	rc = commit_transaction(dh);

    if (rc == 0) {
	dh->changed = 0;
	dh->action_in_prog = 0;
	dh->action_cancelled = 0;
    }

    return rc;
}

int dbats_abort(dbats_handler *dh)
{
    if (!dh->action_in_prog) return 0;
    dbats_log(LOG_FINE, "dbats_abort %u", dh->tslice[0]->time);

    free_isset(dh);

    for (int bid = 0; bid < dh->cfg.num_bundles; bid++) {
	clear_tslice(dh, bid);
    }

    dh->changed = 0;
    dh->action_in_prog = 0;
    dh->action_cancelled = 0;
    return abort_transaction(dh);
}

/*************************************************************************/

int dbats_close(dbats_handler *dh)
{
    int rc = 0;
    int myrc = 0;
    if (!dh->is_open)
	return EINVAL;
    dh->is_open = 0;

    if (dh->action_cancelled) {
	dbats_log(LOG_INFO, "aborting cancelled transaction");
	rc = dbats_abort(dh);
    } else if (dh->action_in_prog) {
	rc = dbats_commit(dh);
    }
    if (myrc == 0 && rc != 0) myrc = rc;

    if (dh->emulate_exclusive) {
	rc = commit_transaction(dh); // exclusive txn
	if (myrc == 0 && rc != 0) myrc = rc;
    }

    for (int bid = 0; bid < dh->cfg.num_bundles; bid++) {
	free(dh->tslice[bid]);
	dh->tslice[bid] = NULL;
    }

    if (dh->db_get_buf) free(dh->db_get_buf);
    if (dh->bundle) free(dh->bundle);
    if (dh->tslice) free(dh->tslice);

    // BDB v4 requires explicit close of each db
    dh->dbMeta->close(dh->dbMeta, 0);
    if (dh->dbBundle)
	dh->dbBundle->close(dh->dbBundle, 0);
    dh->dbKeyname->close(dh->dbKeyname, 0);
    dh->dbKeyid->close(dh->dbKeyid, 0);
    dh->dbData->close(dh->dbData, 0);
    dh->dbIsSet->close(dh->dbIsSet, 0);

    rc = dh->dbenv->close(dh->dbenv, 0);
    if (myrc == 0 && rc != 0) myrc = rc;

    if (dh->state_decompress)
	free(dh->state_decompress);
    if (dh->state_compress)
	free(dh->state_compress);
    free(dh);
    return myrc;
}

/*************************************************************************/

static const time_t time_base = 259200; // 00:00 on first sunday of 1970, UTC

uint32_t dbats_normalize_time(const dbats_handler *dh, int bid, uint32_t *t)
{
    *t -= (*t - time_base) % dh->bundle[bid].period;
    return *t;
}

/*************************************************************************/

static int load_isset(dbats_handler *dh, fragkey_t *dbkey, uint8_t **dest,
    uint32_t flags)
{
    static uint8_t *buf = NULL; // can recycle memory across calls
    int rc;

    if (!buf && !(buf = emalloc(vec_size(ENTRIES_PER_FRAG), "is_set")))
	return errno ? errno : ENOMEM; // error

    rc = raw_db_get(dh, dh->dbIsSet, dbkey, sizeof(*dbkey),
	buf, vec_size(ENTRIES_PER_FRAG), NULL, flags);

    if (rc == DB_NOTFOUND) {
	if (ntohl(dbkey->time) == dh->tslice[ntohl(dbkey->bid)]->time) {
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

    *dest = buf;
    buf = NULL;

    if (ntohl(dbkey->bid) == 0 && dh->active_last_data < ntohl(dbkey->time))
	dh->active_last_data = ntohl(dbkey->time);

    return 0; // ok
}

static int load_frag(dbats_handler *dh, uint32_t t, int bid,
    uint32_t frag_id)
{
    // load fragment
    fragkey_t dbkey = { htonl(bid), htonl(t), htonl(frag_id) };
    int rc;
    uint32_t flags = dh->cfg.readonly ? 0 : DB_RMW;

    assert(!dh->tslice[bid]->is_set[frag_id]);
    rc = load_isset(dh, &dbkey, &dh->tslice[bid]->is_set[frag_id], flags);
    if (rc == DB_NOTFOUND) return 0; // no match
    if (rc != 0) return rc; // error

    if (dh->db_get_buf_len < fragsize(dh) + QLZ_OVERHEAD) {
	if (dh->db_get_buf) free(dh->db_get_buf);
	dh->db_get_buf_len = fragsize(dh) + QLZ_OVERHEAD;
	dh->db_get_buf = emalloc(dh->db_get_buf_len, "get buffer");
	if (!dh->db_get_buf) return errno ? errno : ENOMEM;
    }

    uint32_t value_len;
    rc = raw_db_get(dh, dh->dbData, &dbkey, sizeof(dbkey), dh->db_get_buf,
	dh->db_get_buf_len, &value_len, flags);
    if (rc != 0) {
	if (rc == DB_NOTFOUND) return 0; // no match
	return rc; // error
    }

    int compressed = ((dbats_frag*)dh->db_get_buf)->compressed;
    uint32_t len = !compressed ? fragsize(dh) :
	qlz_size_decompressed((void*)(dh->db_get_buf+1));
    void *ptr = malloc(len);
    if (!ptr) {
	rc = errno;
	dbats_log(LOG_ERROR, "Can't allocate %u bytes for frag "
	    "t=%u, bid=%u, frag_id=%u", len, t, bid, frag_id);
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
	dbats_log(LOG_VERYFINE, "decompressed frag t=%u, bid=%u, frag_id=%u: "
	    "%u -> %u (%.1f%%)",
	    t, bid, frag_id, value_len-1, len, 100.0*len/(value_len-1));

    } else {
	// copy fragment
	// XXX TODO: don't memcpy; get directly into ptr
	memcpy(ptr, dh->db_get_buf, len);
	dbats_log(LOG_VERYFINE, "copied frag t=%u, bid=%u, frag_id=%u",
	    t, bid, frag_id);
    }

    dh->tslice[bid]->frag[frag_id] = ptr;

    dbats_tslice *tslice = dh->tslice[bid];
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;

    return 0;
}

// Instantiate dh->is_set[*][frag_id]
static int instantiate_isset_frags(dbats_handler *dh, int fid)
{
    if (dh->cfg.readonly)
	return 0; // dh->is_set is never needed in readonly mode

    int tn = (dh->active_end - dh->active_start) / dh->bundle[0].period + 1;

    if (!dh->is_set) {
	dh->is_set = ecalloc(tn, sizeof(uint8_t**), "dh->is_set");
	if (!dh->is_set) return errno ? errno : ENOMEM;
    }

    uint32_t t = dh->active_start;
    for (int ti = 0; ti < tn; ti++, t += dh->bundle[0].period) {
	fragkey_t dbkey = { 0, htonl(t), 0 };
	if (!dh->is_set[ti]) {
	    dh->is_set[ti] = ecalloc(MAX_NUM_FRAGS, sizeof(uint8_t*),
		"dh->is_set[ti]");
	    if (!dh->is_set[ti]) return errno ? errno : ENOMEM;
	}

	if (t == dh->tslice[0]->time && dh->tslice[0]->is_set[fid]) {
	    assert(!dh->is_set[ti][fid] || dh->is_set[ti][fid] == dh->tslice[0]->is_set[fid]);
	    dh->is_set[ti][fid] = dh->tslice[0]->is_set[fid]; // share
	} else if (!dh->is_set[ti][fid]) {
	    dbkey.frag_id = htonl(fid);
	    assert(!dh->is_set[ti][fid]);
	    int is_current = (t == dh->tslice[0]->time);
	    int rc = load_isset(dh, &dbkey, &dh->is_set[ti][fid],
		is_current ? DB_RMW : 0);
	    if (rc != 0 && rc != DB_NOTFOUND)
		return rc; // error
	    if (is_current) {
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

static void set_priority(dbats_handler *dh, uint32_t priority)
{
    if (!dh->n_txn) return;
#if HAVE_DB_SET_PRIORITY
    int rc = current_txn(dh)->set_priority(current_txn(dh), priority);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "set_priority %u: %s", priority, db_strerror(rc));
    } else {
	dbats_log(LOG_FINE, "set_priority %u", priority);
    }
#else
    static int warned = 0;
    if (!warned)
	dbats_log(LOG_WARNING, "can't set_priority with this version of BDB");
    warned = 1;
#endif
}

int dbats_select_time(dbats_handler *dh, uint32_t time_value, uint32_t flags)
{
    int rc;
    uint32_t min_time = 0;
    uint32_t max_time = 0;
    int tries_limit = 60;

    dbats_log(LOG_FINE, "select_time %u", time_value);

    if ((rc = dbats_commit(dh)) != 0)
	return rc;

    dh->preload = !!(flags & DBATS_PRELOAD);

restart:
    if ((rc = begin_transaction(dh, "tslice txn")) != 0)
	return rc;
    dh->action_in_prog = 1;
    dh->action_cancelled = 0;

    if (!dh->cfg.readonly) {
	// DB_RMW will block other writers trying to select_time().
	// DB_READ_COMMITTED won't hold a read lock, so won't block anyone.
	rc = CFG_GET(dh, "min_keep_time", dh->min_keep_time,
	    dh->serialize ? DB_RMW : DB_READ_COMMITTED);
	if (rc == DB_LOCK_DEADLOCK) {
	    if (--tries_limit > 0) goto retry;
	    dbats_log(LOG_ERROR, "select_time %u: too many deadlocks",
		time_value);
	}
	if (rc != 0) goto abort;
	dbats_log(LOG_FINE, "select_time %u: got %s = %u", time_value,
	    "min_keep_time", dh->min_keep_time);

	dbats_get_end_time(dh, 0, &dh->end_time);
    }

    for (int bid = 0; ; bid++) {
	rc = read_bundle_info(dh, bid, 0);
	if (rc != 0) {
	    if (rc == DB_NOTFOUND && bid > 0) break; // no more bundles
	    goto abort;
	}
	dh->cfg.num_bundles = bid + 1;
    }

    for (int bid = 0; bid < dh->cfg.num_bundles; bid++) {
	dbats_tslice *tslice = dh->tslice[bid];
	uint32_t t = time_value;

	dbats_normalize_time(dh, bid, &t);

	if (!dh->cfg.readonly) {
	    if (t < dh->min_keep_time) {
		dbats_log(LOG_ERROR, "select_time %u: illegal attempt to set "
		    "value in bundle %d at time %u before series limit %u",
		    time_value, bid, t, dh->min_keep_time);
		rc = EINVAL;
		goto abort;
	    }
	}

	if (dh->cfg.exclusive) {
	    if (tslice->time == t) {
		// keep relevant fragments
		dbats_log(LOG_FINE, "select_time %u, bid=%d: already loaded",
		    t, bid);
	    } else {
		// free obsolete fragments
		clear_tslice(dh, bid);
	    }
	}

	tslice->time = t;
	if (bid == 0)
	    dh->active_last_data = t;

	if (!min_time || t < min_time)
	    min_time = t;
	t += dh->bundle[bid].period - dh->bundle[0].period;
	if (t > max_time)
	    max_time = t;
    }
    dh->active_start = min_time;
    dh->active_end = max_time;

    if (!dh->cfg.readonly) {
	// Make priority depend on time, so a writer of realtime data always
	// beats a writer of historical data.
	set_priority(dh, dh->tslice[0]->time / dh->bundle[0].period);
    }

    if (!dh->preload)
	return 0;

    uint32_t num_keys;
    rc = dbats_num_keys(dh, &num_keys);
    if (rc != 0) goto abort;

    for (int bid = 0; bid < dh->cfg.num_bundles; bid++) {
	dbats_tslice *tslice = dh->tslice[bid];
	uint32_t t = tslice->time;
	int loaded = 0;

	tslice->num_frags = div_ceil(num_keys, ENTRIES_PER_FRAG);
	for (uint32_t frag_id = 0; frag_id < tslice->num_frags; frag_id++) {
	    rc = load_frag(dh, t, bid, frag_id);
	    if (rc == DB_LOCK_DEADLOCK) {
		if (--tries_limit > 0) goto retry;
		dbats_log(LOG_ERROR, "select_time %u: too many deadlocks",
		    time_value);
	    }
	    if (rc != 0) goto abort;
	    if (tslice->frag[frag_id])
		loaded++;
	}
	dbats_log(LOG_FINE, "select_time %u: bid=%d, loaded %u/%u fragments",
	    time_value, bid, loaded, tslice->num_frags);
    }

    for (int frag_id = 0; frag_id < dh->tslice[0]->num_frags; frag_id++) {
	rc = instantiate_isset_frags(dh, frag_id);
	if (rc == DB_LOCK_DEADLOCK) {
	    if (--tries_limit > 0) goto retry;
	    dbats_log(LOG_ERROR, "select_time %u: too many deadlocks",
		time_value);
	}
	if (rc != 0) goto abort;
    }

    return 0;

abort:
    dbats_abort(dh); // tslice txn
    return rc;

retry:
    dbats_abort(dh); // tslice txn
    goto restart;
}

/*************************************************************************/

int dbats_bulk_get_key_id(dbats_handler *dh, uint32_t n_keys,
    const char * const *keys, uint32_t *key_ids, uint32_t flags)
{
    int rc;
    int my_txn = dh->cfg.exclusive && dh->n_txn == 0;

    if (my_txn)
	begin_transaction(dh, "key txn");

    for (int i = 0; i < n_keys; i++) {
	rc = raw_db_get(dh, dh->dbKeyname, keys[i], strlen(keys[i]),
	    &key_ids[i], sizeof(key_ids[i]), NULL, 0);
	if (rc == 0) {
	    // found it
	    dbats_log(LOG_FINEST, "Found key #%u: %s", key_ids[i], keys[i]);

	} else if (rc != DB_NOTFOUND) {
	    // error
	    goto abort;

	} else if (!(flags & DBATS_CREATE)) {
	    // didn't find, and can't create
	    dbats_log(LOG_FINE, "Key not found: %s", keys[i]);
	    goto abort;

	} else {
	    // create
	    db_recno_t recno;
	    DBT_in(dbt_keyname, (void*)keys[i], strlen(keys[i]));
	    DBT_out(dbt_keyrecno, &recno, sizeof(recno)); // put() will fill recno
	    rc = dh->dbKeyid->put(dh->dbKeyid, current_txn(dh), &dbt_keyrecno,
		&dbt_keyname, DB_APPEND);
	    if (rc != 0) {
		if (dh->action_in_prog && rc == DB_LOCK_DEADLOCK)
		    dh->action_cancelled = 1;
		dbats_log(LOG_ERROR, "Error creating keyid for %s: %s",
		    keys[i], db_strerror(rc));
		goto abort;
	    }
	    key_ids[i] = recno - 1;
	    if (key_ids[i] >= MAX_NUM_FRAGS * ENTRIES_PER_FRAG) {
		dbats_log(LOG_ERROR, "Out of space for key %s", keys[i]);
		rc = ENOMEM;
		goto abort;
	    }

	    DBT_in(dbt_keyid, &key_ids[i], sizeof(key_ids[i]));
	    rc = dh->dbKeyname->put(dh->dbKeyname, current_txn(dh),
		&dbt_keyname, &dbt_keyid, DB_NOOVERWRITE);
	    if (rc != 0) {
		if (dh->action_in_prog && rc == DB_LOCK_DEADLOCK)
		    dh->action_cancelled = 1;
		dbats_log(LOG_ERROR, "Error creating keyname %s -> %u: %s",
		    keys[i], key_ids[i], db_strerror(rc));
		goto abort;
	    }
	    dbats_log(LOG_FINEST, "Assigned key #%u: %s", key_ids[i], keys[i]);
	}
    }

    if (my_txn)
	rc = commit_transaction(dh); // key txn
    if (rc == 0) return 0;
abort:
    if (my_txn)
	abort_transaction(dh); // key txn
    return rc;
}

int dbats_get_key_id(dbats_handler *dh, const char *key,
    uint32_t *key_id_p, uint32_t flags)
{
    return dbats_bulk_get_key_id(dh, 1, &key, key_id_p, flags);
}

int dbats_get_key_name(dbats_handler *dh, uint32_t key_id, char *namebuf)
{
    int rc;
    db_recno_t recno = key_id + 1;
    uint32_t value_len;

    rc = raw_db_get(dh, dh->dbKeyid, &recno, sizeof(recno),
	namebuf, DBATS_KEYLEN - 1, &value_len, DB_READ_COMMITTED);
    if (rc != 0) return rc;
    namebuf[value_len] = '\0';
    return 0;
}

/*************************************************************************/

static int instantiate_frag_func(dbats_handler *dh, int bid, uint32_t frag_id)
{
    int rc;
    dbats_tslice *tslice = dh->tslice[bid];

    if (!dh->preload || dh->cfg.readonly) {
	// Try to load the fragment.
	rc = load_frag(dh, tslice->time, bid, frag_id);
	if (rc != 0) // error
	    return rc;
	if (tslice->frag[frag_id]) { // found it
	    if (bid == 0)
		rc = instantiate_isset_frags(dh, frag_id);
	    return rc;
	}
	if (dh->cfg.readonly) // didn't find it and can't create it
	    return DB_NOTFOUND; // fragment not found
    }

    // Allocate a new fragment.
    dbats_log(LOG_VERYFINE, "grow tslice for time %u, bid %d, frag_id %u",
	tslice->time, bid, frag_id);

    size_t len = fragsize(dh);
    if (!(tslice->frag[frag_id] = ecalloc(1, len, "tslice->frag[frag_id]"))) {
	return errno ? errno : ENOMEM;
    }

    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;
    if (bid == 0) {
	rc = instantiate_isset_frags(dh, frag_id);
	if (rc != 0) return rc;
    }

    dbats_log(LOG_VERYFINE, "Grew tslice to %u elements",
	tslice->num_frags * ENTRIES_PER_FRAG);

    return 0;
}

// inline version handles the common case without a function call
static inline int instantiate_frag(dbats_handler *dh, int bid, uint32_t frag_id)
{
    return dh->tslice[bid]->frag[frag_id] ?
	0 : // We already have the fragment.
	instantiate_frag_func(dh, bid, frag_id);
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

    if (!dh->tslice[0]->time || !dh->action_in_prog) {
	dbats_log(LOG_ERROR, "dbats_set() without dbats_select_time()");
	return -1;
    }

retry:
    if (TXN_PER_OP && !dh->serialize)
	begin_transaction(dh, "set txn");

    if ((rc = instantiate_frag(dh, 0, frag_id)) != 0) {
	if (TXN_PER_OP && !dh->serialize) {
	    abort_transaction(dh); // set txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }

    uint8_t was_set = vec_test(dh->tslice[0]->is_set[frag_id], offset);
    if (was_set & !dh->cfg.updatable) {
	dbats_log(LOG_ERROR, "value is already set: t=%" PRIu32 " keyid=%d",
	    dh->tslice[0]->time, key_id);
	return DB_KEYEXIST;
    }
    dbats_value *oldvaluep = valueptr(dh, 0, frag_id, offset);

    // For each bid, aggregate *valuep into tslice[bid].
    for (int bid = 1; bid < dh->cfg.num_bundles; bid++) {

	if (dh->bundle[bid].keep > 0) {
	    // Skip if the time is outside of bundle's keep limit
	    uint32_t keep_period = (dh->bundle[bid].keep-1) * dh->bundle[bid].period;
	    if (keep_period < dh->end_time &&
		dh->tslice[bid]->time < dh->end_time - keep_period)
		    continue;
	}

	if ((rc = instantiate_frag(dh, bid, frag_id)) != 0) {
	    if (TXN_PER_OP && !dh->serialize) {
		abort_transaction(dh); // set txn
		if (rc == DB_LOCK_DEADLOCK) goto retry;
	    }
	    return rc;
	}

	uint8_t changed = 0;
	uint8_t failed = 0;
	dbats_value *aggval = valueptr(dh, bid, frag_id, offset);
	uint32_t aggstart = dh->tslice[bid]->time;
	uint32_t aggend = min(dh->active_last_data,
	    aggstart + dh->bundle[bid].period - dh->bundle[0].period);

	// Count the number of steps contributing to the aggregate.
	int n = 0;
	{
	    int ti = (aggstart - dh->active_start) / dh->bundle[0].period;
	    uint32_t t = aggstart;
	    while (t <= aggend) {
		if (t == dh->tslice[0]->time ||
		    vec_test(dh->is_set[ti][frag_id], offset))
			n++;
		ti++;
		t += dh->bundle[0].period;
	    }
	}

	dbats_log(LOG_FINEST, "agg %d: aggval=%" PRIval " n=%d",
	    bid, aggval[0], n); // XXX

	for (int i = 0; i < dh->cfg.values_per_entry; i++) {
	    if (was_set && valuep[i] == oldvaluep[i]) {
		continue; // value did not change; no need to change agg value
	    }
	    switch (dh->bundle[bid].func) {
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
		    int ti = (t - dh->active_start) / dh->bundle[0].period;
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
			t -= dh->bundle[0].period;
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
		if (dh->bundle[bid].func == DBATS_AGG_AVG)
		    sprintf(aggbuf, "%f", ((double*)aggval)[0]);
		else
		    sprintf(aggbuf, "%" PRIval, aggval[0]);
		dbats_log(level, "%s set value bid=%d frag_id=%u "
		    "offset=%" PRIu32 " value_len=%u aggval=%s",
		    failed ? "Failed to" : "Successfully",
		    bid, frag_id, offset, dh->cfg.entry_size, aggbuf);
	    }
	}
	if (changed) {
	    // XXX if (n >= xff * steps)
	    vec_set(dh->tslice[bid]->is_set[frag_id], offset);
	    dh->tslice[bid]->frag_changed[frag_id] = 1;
	} else if (failed) {
	    if (TXN_PER_OP && !dh->serialize)
		abort_transaction(dh); // set txn
	    return failed;
	}
    }

    // Set value at bid 0 (after aggregations because aggregations need
    // both old and new values)
    memcpy(oldvaluep, valuep, dh->cfg.entry_size);
    vec_set(dh->tslice[0]->is_set[frag_id], offset);

    if (!dh->changed) {
	// Once we've started writing, increase priority over other processes
	// that haven't yet done any writing.
	set_priority(dh, (dh->tslice[0]->time + INT32_MAX) / dh->bundle[0].period);
	dh->changed = 1;
    }
    dh->tslice[0]->frag_changed[frag_id] = 1;

    dbats_log(LOG_VERYFINE, "Succesfully set value "
	"bid=%d frag_id=%u offset=%" PRIu32 " value_len=%u value=%" PRIval,
	0, frag_id, offset, dh->cfg.entry_size, valuep[0]);

    if (TXN_PER_OP && !dh->serialize)
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
    const dbats_value **valuepp, int bid)
{
    int rc;
    dbats_tslice *tslice = dh->tslice[bid];

    if (!dh->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if (!dh->tslice[0]->time || !dh->action_in_prog) {
	dbats_log(LOG_ERROR, "dbats_get() without dbats_select_time()");
	return -1;
    }

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

retry:
    if (TXN_PER_OP && !dh->serialize)
	begin_transaction(dh, "get txn");

    if ((rc = instantiate_frag(dh, bid, frag_id)) == 0) {
	if (!vec_test(tslice->is_set[frag_id], offset)) {
	    char keyname[DBATS_KEYLEN] = "";
	    dbats_get_key_name(dh, key_id, keyname);
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, bid, keyname);
	    *valuepp = NULL;
	    if (TXN_PER_OP && !dh->serialize)
		commit_transaction(dh); // get txn
	    return DB_NOTFOUND;
	}
	if (dh->bundle[bid].func == DBATS_AGG_AVG) {
	    double *dval = (double*)valueptr(dh, bid, frag_id, offset);
	    static dbats_value *avg_buf = NULL;
	    if (!avg_buf) {
		avg_buf = emalloc(dh->cfg.entry_size * dh->cfg.values_per_entry,
		    "avg_buf");
		if (!avg_buf) {
		    if (TXN_PER_OP && !dh->serialize)
			abort_transaction(dh); // get txn
		    return errno ? errno : ENOMEM;
		}
	    }
	    for (int i = 0; i < dh->cfg.values_per_entry; i++)
		avg_buf[i] = dval[i] + 0.5;
	    *valuepp = avg_buf;
	} else {
	    *valuepp = valueptr(dh, bid, frag_id, offset);
	}
	dbats_log(LOG_VERYFINE, "Succesfully read value off=%" PRIu32 " len=%u",
	    offset, dh->cfg.entry_size);
	if (TXN_PER_OP && !dh->serialize)
	    commit_transaction(dh); // get txn
	return 0;

    } else if (rc == DB_NOTFOUND) {
	char keyname[DBATS_KEYLEN] = "";
	dbats_get_key_name(dh, key_id, keyname);
	dbats_log(LOG_WARNING, "Value unset (f): %u %d %s",
	    tslice->time, bid, keyname);
	*valuepp = NULL;
	if (TXN_PER_OP && !dh->serialize)
	    commit_transaction(dh); // get txn
	return rc;

    } else {
	if (TXN_PER_OP && !dh->serialize) {
	    abort_transaction(dh); // get txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }
}

int dbats_get_double(dbats_handler *dh, uint32_t key_id,
    const double **valuepp, int bid)
{
    int rc;
    dbats_tslice *tslice = dh->tslice[bid];

    if (!dh->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if (dh->bundle[bid].func != DBATS_AGG_AVG) {
	dbats_log(LOG_ERROR, "Aggregation %d is not a double", bid);
	*valuepp = NULL;
	return -1;
    }

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

retry:
    if (TXN_PER_OP && !dh->serialize)
	begin_transaction(dh, "get txn");

    if ((rc = instantiate_frag(dh, bid, frag_id)) == 0) {
	if (!vec_test(tslice->is_set[frag_id], offset)) {
	    char keyname[DBATS_KEYLEN] = "";
	    dbats_get_key_name(dh, key_id, keyname);
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, bid, keyname);
	    *valuepp = NULL;
	    if (TXN_PER_OP && !dh->serialize)
		commit_transaction(dh); // get txn
	    return DB_NOTFOUND;
	}
	*valuepp = (double*)valueptr(dh, bid, frag_id, offset);
	dbats_log(LOG_VERYFINE, "Succesfully read value off=%" PRIu32 " len=%u",
	    offset, dh->cfg.entry_size);
	if (TXN_PER_OP && !dh->serialize)
	    commit_transaction(dh); // get txn
	return 0;

    } else if (rc == DB_NOTFOUND) {
	char keyname[DBATS_KEYLEN] = "";
	dbats_get_key_name(dh, key_id, keyname);
	dbats_log(LOG_WARNING, "Value unset (f): %u %d %s",
	    tslice->time, bid, keyname);
	*valuepp = NULL;
	if (TXN_PER_OP && !dh->serialize)
	    commit_transaction(dh); // get txn
	return rc;

    } else {
	if (TXN_PER_OP && !dh->serialize) {
	    abort_transaction(dh); // get txn
	    if (rc == DB_LOCK_DEADLOCK) goto retry;
	}
	return rc;
    }
}

int dbats_get_by_key(dbats_handler *dh, const char *key,
    const dbats_value **valuepp, int bid)
{
    int rc;
    uint32_t key_id;

    if ((rc = dbats_get_key_id(dh, key, &key_id, 0)) != 0) {
	return rc;
    }
    return dbats_get(dh, key_id, valuepp, bid);
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
    rc = raw_cursor_get(dh, dh->dbKeyname, dh->keyname_cursor,
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
    rc = raw_cursor_get(dh, dh->dbKeyid, dh->keyid_cursor,
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

const dbats_bundle_info *dbats_get_bundle_info(dbats_handler *dh, int bid)
    { return &dh->bundle[bid]; }

/*************************************************************************/

void dbats_stat_print(const dbats_handler *dh) {
    int rc;
    
    if ((rc = dh->dbMeta->stat_print(dh->dbMeta, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping Meta stats: %s", db_strerror(rc));
    if (dh->dbBundle)
	if ((rc = dh->dbBundle->stat_print(dh->dbBundle, DB_FAST_STAT)) != 0)
	    dbats_log(LOG_ERROR, "dumping Bundle stats: %s", db_strerror(rc));
    if ((rc = dh->dbKeyname->stat_print(dh->dbKeyname, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping Keyname stats: %s", db_strerror(rc));
    if ((rc = dh->dbKeyid->stat_print(dh->dbKeyid, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping Keyid stats: %s", db_strerror(rc));
    if ((rc = dh->dbData->stat_print(dh->dbData, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping Data stats: %s", db_strerror(rc));
    if ((rc = dh->dbIsSet->stat_print(dh->dbIsSet, DB_FAST_STAT)) != 0)
	dbats_log(LOG_ERROR, "dumping IsSet stats: %s", db_strerror(rc));
}

