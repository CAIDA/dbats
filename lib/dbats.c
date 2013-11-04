/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lessed General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 */

#define _POSIX_C_SOURCE 200809L

#include <unistd.h>
#include "uint.h"
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <db.h>
#include <assert.h>
#include <arpa/inet.h> // htonl() etc

#include <db.h>
#include "dbats.h"
#include "quicklz.h"
#include "config.h"

// Limits
#define ENTRIES_PER_FRAG    10000 // number of entries in a fragment
#define MAX_NUM_FRAGS       16384 // max number of fragments in a tslice
#define MAX_NUM_BUNDLES        16 // max number of time series bundles

#define DBATS_ISSET_ALL   1 // all bits of is_set are set
#define DBATS_ISSET_NONE  2 // all bits of is_set are clear

/*************************************************************************/
// compile-time assertion (works anywhere a declaration is allowed)
#define ct_assert(expr, label)  enum { ASSERT_ERROR__##label = 1/(!!(expr)) };

ct_assert((sizeof(double) == sizeof(uint64_t)), double_is_not_64_bits)


/*************************************************************************/

// bit vector
#define vec_size(N)       (((N)+7)/8)                    // size for N bits
#define vec_set(vec, i)   (vec[(i)/8] |= (1<<((i)%8)))   // set the ith bit
#define vec_reset(vec, i) (vec[(i)/8] &= ~(1<<((i)%8)))  // reset the ith bit
#define vec_test(vec, i)  (vec && (vec[(i)/8] & (1<<((i)%8)))) // test ith bit

#define valueptr(ds, bid, frag_id, offset) \
    ((dbats_value*)(&ds->tslice[bid]->frag[frag_id]->data[offset * dh->cfg.entry_size]))

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

typedef struct {
    DB *db;
    DBC *dbc;
} cursor_wrapper;


#define KTKEY_NODENAME_MAXLEN 255
#define KTKEY_MAX_LEVEL 0xFF
#define KTKEY_MAX_NODE 0x7FFFFFFF

// dbKeytree table's key
typedef struct {
    uint32_t parent; // big endian
    char nodename[KTKEY_NODENAME_MAXLEN];
} keytree_key;
#define KTKEY_SIZE(nodenamelen) (sizeof(uint32_t) + nodenamelen)

#define cswap32(x) ( \
    (((x)&0x000000FF)<<24) | \
    (((x)&0x0000FF00)<<8) | \
    (((x)&0x00FF0000)>>8) | \
    (((x)&0xFF000000)>>24))

#define cswap64(x) ( \
    (((x)&0x00000000000000FF)<<56) | \
    (((x)&0x000000000000FF00)<<40) | \
    (((x)&0x0000000000FF0000)<<24) | \
    (((x)&0x00000000FF000000)<<8) | \
    (((x)&0x000000FF00000000)>>8) | \
    (((x)&0x0000FF0000000000)>>24) | \
    (((x)&0x00FF000000000000)>>40) | \
    (((x)&0xFF00000000000000)>>56))

#ifdef WORDS_BIGENDIAN
#define chtonl(x) (x)
#else
#define chtonl(x) cswap32(x)
#endif

const uint32_t KTID_IS_NODE      = chtonl(DBATS_KEY_IS_PREFIX);


struct dbats_snapshot {
    dbats_handler *dh;
    dbats_tslice **tslice;     // a tslice for each time series bundle
    uint32_t active_start;     // first timestamp in active period
                               // (corresponding to is_set[0])
    uint32_t active_end;       // last timestamp in active period
    uint32_t active_last_data; // last timestamp in active period with data
    uint32_t end_time;         // latest time for which a value has been set
    uint32_t min_keep_time;    // earliest time for which a value can be set
    uint8_t ***is_set;         // each is_set[timeindex][fragid] is a bitvector
                               // indicating which cols have values
    DB_TXN *txn;               // transaction
    void *state_compress;
    void *state_decompress;
    uint8_t *db_get_buf;       // buffer for data fragment
    uint8_t preload;           // load frags when tslice is selected
    uint8_t changed;           // has changes that need to be written to db?
    uint8_t readonly;
    size_t db_get_buf_len;
    uint16_t num_bundles;      // Number of time series bundles
    dbats_value *valbuf;       // buffer for holding modified value
};

struct dbats_handler {
    dbats_config cfg;
    const char *path;          // path of BDB environment directory
    uint8_t is_open;
    uint8_t swapped;           // db was created with opposite byte order?
    uint8_t serialize;         // allow only one writer at a time
    DB_ENV *dbenv;             // DB environment
    DB *dbConfig;              // config parameters
    DB *dbBundle;              // bundle parameters
    DB *dbKeytree;             // {level, parent, nodename} -> node id or key id
    DB *dbKeyid;               // recno -> key name (note: keyid == recno - 1)
    DB *dbValues;              // {bid, time, frag_id} -> value fragment
    DB *dbIsSet;               // {bid, time, frag_id} -> is_set fragment
    DB *dbSequence;            // BDB sequences
    dbats_bundle_info *bundle; // parameters for each time series bundle
    DB_TXN *txn;               // transaction
    void *userdata;            // user data storage
};

// key tree lookup state
typedef struct {
    uint32_t id;     // 1 bit flag (0=keyid, 1=nodeid); 31 bit id.  Big endian.
    char *start;     // start of substr of dh->pattern that matched this node
    uint32_t gnlen;  // length of glob node in dh->pattern
    uint32_t anlen;  // length of actual node in key.nodename
    uint32_t pfxlen; // length of literal prefix of nodename glob
    keytree_key key; // db key for this node
    cursor_wrapper cursor;     // db cursor for globbing this node
} kt_node_t;

struct dbats_keytree_iterator {
    dbats_handler *dh;
    DB_TXN *txn;                        // transaction
    uint8_t txn_is_mine;                // flag
    char *pattern;                      // pattern
    kt_node_t nodes[KTKEY_MAX_LEVEL+1]; // node stack
    int kt_levels;                      // # of levels in nodes
    int lvl;                            // current level
    DB_SEQUENCE *ktseq;                 // for generating node numbers
};

struct dbats_keyid_iterator {
    dbats_handler *dh;
    dbats_snapshot *ds;
    cursor_wrapper cursor;
};

const char *dbats_agg_func_label[] = {
    "data", "min", "max", "avg", "last", "sum"
};

/************************************************************************
 * utilities
 ************************************************************************/

// Construct a DBT for sending data to DB.
#define DBT_init_in(_var, _data, _size) \
    do { \
	memset(&_var, 0, sizeof(DBT)); \
	_var.data = _data; \
	_var.size = _size; \
	_var.flags = DB_DBT_USERMEM; \
    } while (0)

// Construct a DBT for sending data to DB.
#define DBT_in(_var, _data, _size) DBT _var; DBT_init_in(_var, _data, _size)


// Construct a DBT for receiving data from DB.
#define DBT_init_out(_var, _data, _ulen) \
    do { \
	memset(&_var, 0, sizeof(DBT)); \
	_var.data = _data; \
	_var.ulen = _ulen; \
	_var.flags = DB_DBT_USERMEM; \
    } while (0)

// Construct a DBT for receiving data from DB.
#define DBT_out(_var, _data, _ulen) DBT _var; DBT_init_out(_var, _data, _ulen)


// Construct a DBT for receiving nothing from DB.
#define DBT_init_null(_var) \
    do { \
	memset(&_var, 0, sizeof(DBT)); \
	_var.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL; \
    } while (0)

// Construct a DBT for receiving nothing from DB.
#define DBT_null(_var) DBT _var; DBT_init_null(_var)

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
	dbats_log(DBATS_LOG_WARN, "Can't allocate %zu bytes for %s", sz, msg);
	errno = err;
    }
    return ptr;
}

static void *ecalloc(size_t n, size_t sz, const char *msg)
{
    void *ptr = calloc(n, sz);
    if (!ptr) {
	int err = errno;
	dbats_log(DBATS_LOG_WARN, "Can't alloc %zu*%zu bytes for %s", n, sz, msg);
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

static inline int begin_transaction(dbats_handler *dh, DB_TXN *parent,
    DB_TXN **childp, const char *name)
{
    if (dh->cfg.no_txn) return 0;
    dbats_log(DBATS_LOG_FINE, "begin txn: %s", name);
    int rc;
    rc = dh->dbenv->txn_begin(dh->dbenv, parent, childp, 0);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "begin txn: %s: %s", name, db_strerror(rc));
	return rc;
    }
    (*childp)->set_name(*childp, name);
    return 0;
}

static inline int commit_transaction(dbats_handler *dh, DB_TXN *txn)
{
    int rc;
    const char *name;
    txn->get_name(txn, &name);
    dbats_log(DBATS_LOG_FINE, "commit txn: %s", name);
    if ((rc = txn->commit(txn, 0)) != 0)
	dbats_log(DBATS_LOG_ERR, "commit txn: %s: %s", name, db_strerror(rc));
    rc = dh->dbenv->txn_checkpoint(dh->dbenv, 256, 0, 0);
    if (rc != 0)
	dbats_log(DBATS_LOG_ERR, "txn_checkpoint: %s", db_strerror(rc));
    return rc;
}

static int abort_transaction(dbats_handler *dh, DB_TXN *txn)
{
    int rc;
    const char *name;
    txn->get_name(txn, &name);
    dbats_log(DBATS_LOG_FINE, "abort txn: %s", name);
    if ((rc = txn->abort(txn)) != 0)
	dbats_log(DBATS_LOG_ERR, "abort txn: %s: %s", name, db_strerror(rc));
    return rc;
}

static int raw_db_open(dbats_handler *dh, DB **dbp, const char *name,
    DBTYPE dbtype, uint32_t flags, int mode, int use_txn)
{
    int rc;

    if ((rc = db_create(dbp, dh->dbenv, 0)) != 0) {
	dbats_log(DBATS_LOG_ERR, "Error creating DB dh %s/%s: %s",
	    dh->path, name, db_strerror(rc));
	return rc;
    }
    (*dbp)->app_private = dh;

    if (dh->cfg.exclusive) {
#if HAVE_DB_SET_LK_EXCLUSIVE
	if ((rc = (*dbp)->set_lk_exclusive(*dbp, 1)) != 0) {
	    dbats_log(DBATS_LOG_ERR, "Error obtaining exclusive lock on %s/%s: %s",
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
    if (!(flags & DBATS_EXCLUSIVE))
	dbflags |= DB_THREAD;

    rc = (*dbp)->open(*dbp, use_txn ? dh->txn : NULL, name, NULL,
	dbtype, dbflags, mode);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "Error opening DB %s in %s (mode=%o): %s",
	    name, dh->path, mode, db_strerror(rc));
	(*dbp)->close(*dbp, 0);
	return rc;
    }
    return 0;
}

static void log_db_op(int level, const char *fname, int line, DB *db,
    const char *label, const void *key, uint32_t key_len, const char *msg)
{
    dbats_handler *dh = (dbats_handler*)db->app_private;
    char keybuf[DBATS_KEYLEN] = "";
    if (!key) {
	// don't log key
    } else if (db == dh->dbValues || db == dh->dbIsSet) {
	fragkey_t *fragkey = (fragkey_t*)key;
	sprintf(keybuf, "bid=%d t=%u frag=%u",
	    ntohl(fragkey->bid), ntohl(fragkey->time), ntohl(fragkey->frag_id));
    } else if (db == dh->dbBundle) {
	sprintf(keybuf, "%d", *(uint8_t*)key);
    } else if (db == dh->dbKeytree) {
	keytree_key* ktkey = (keytree_key*)key;
	sprintf(keybuf, "parent=%x name=\"%.*s\"",
	    ntohl(ktkey->parent), key_len - (int)KTKEY_SIZE(0), ktkey->nodename);
    } else if (db == dh->dbConfig) {
	sprintf(keybuf, "\"%.*s\"", key_len, (char*)key);
    } else if (db == dh->dbKeyid) {
	sprintf(keybuf, "#%u", *(db_recno_t*)key);
    }
    const char *dbname;
    db->get_dbname(db, &dbname, NULL);
    dbats_log_func(level, fname, line, "%s: %s: %s: %s",
	label, dbname, keybuf, msg);
}

#define raw_db_set(db, txn, key, klen, value, vlen, dbflags) \
    (raw_db_set)(__FILE__, __LINE__, db, txn, key, klen, value, vlen, dbflags)

static int (raw_db_set)(
    const char *fname, int line,
    DB *db, DB_TXN *txn,
    const void *key, uint32_t key_len,
    const void *value, uint32_t value_len,
    uint32_t dbflags)
{
    log_db_op(DBATS_LOG_FINEST, fname, line, db, "db_set", key, key_len, "");
    int rc;
    dbats_handler *dh = (dbats_handler*)db->app_private;

    if (dh->cfg.readonly) {
	const char *dbname;
	db->get_dbname(db, &dbname, NULL);
	dbats_log(DBATS_LOG_WARN, "Unable to set value in database \"%s\" "
	    "(read-only mode)", dbname);
	return EPERM;
    }

    DBT_in(dbt_key, (void*)key, key_len);
    DBT_in(dbt_data, (void*)value, value_len);

    if ((rc = db->put(db, txn, &dbt_key, &dbt_data, dbflags)) != 0) {
	log_db_op(DBATS_LOG_WARN, fname, line, db, "db_set", key, key_len,
	    db_strerror(rc));
	return rc;
    }
    return 0;
}

#define QLZ_OVERHEAD 400

#define raw_db_get(db, txn, key, klen, value, buflen, vlen_p, dbflags) \
    (raw_db_get)(__FILE__, __LINE__, db, txn, key, klen, value, buflen, vlen_p, dbflags)

// Get a value from the database.
// buflen must contain the length of the memory pointed to by value.
// If value_len_p != NULL, then after the call, *value_len_p will contain the
// length of data written into the memory pointed to by value.
static int (raw_db_get)(const char *fname, int line,
    DB* db, DB_TXN *txn,
    const void *key, uint32_t key_len,
    void *value, size_t buflen, uint32_t *value_len_p,
    uint32_t dbflags)
{
    const char *label =
	(dbflags & DB_RMW) ? "db_get(rw)" :
	(dbflags & DB_READ_COMMITTED) ? "db_get(t)" :
	"db_get(r)";
    log_db_op(DBATS_LOG_FINEST, fname, line, db, label, key, key_len, "");
    int rc;

    DBT_in(dbt_key, (void*)key, key_len);
    DBT_out(dbt_data, value, buflen);
    rc = db->get(db, txn, &dbt_key, &dbt_data, dbflags);
    if (rc == 0) {
	if (value_len_p) {
	    *value_len_p = dbt_data.size;
	} else if (dbt_data.size != buflen) {
	    dbats_log(DBATS_LOG_WARN, "db_get: expected %" PRIu32 ", got %" PRIu32,
		dbt_data.ulen, dbt_data.size);
	}
	return 0;
    }
    log_db_op(rc == DB_NOTFOUND ? DBATS_LOG_FINE : DBATS_LOG_WARN, fname, line, db,
	label, key, key_len, db_strerror(rc));
    if (rc == DB_BUFFER_SMALL) {
	dbats_log(DBATS_LOG_WARN, "db_get: had %" PRIu32 ", needed %" PRIu32,
	    dbt_data.ulen, dbt_data.size);
    }
    return rc;
}

static int raw_cursor_open(DB *db, DB_TXN *txn,
    cursor_wrapper *cw, int flags)
{
    cw->db = db;
    return db->cursor(db, txn, &cw->dbc, flags);
}

static int raw_cursor_close(cursor_wrapper *cw)
{
    int rc = cw->dbc->close(cw->dbc);
    cw->dbc = NULL;
    return rc;
}

#define raw_cursor_get(cw, key, data, flags) \
    (raw_cursor_get)(__FILE__, __LINE__, cw, key, data, flags)

static int (raw_cursor_get)(const char *fname, int line,
    cursor_wrapper *cw, DBT *key, DBT *data, int flags)
{
#if (DB_VERSION_MAJOR < 5)
    // version 4 didn't support DB_READ_COMMITTED in cursor get
    flags = flags & ~DB_READ_COMMITTED;
#endif
    char label[32] = "cursor_get()";
    if (dbats_log_level >= DBATS_LOG_FINEST) {
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
	void *logdata = (op == DB_NEXT || op == DB_PREV || op == DB_FIRST ||
	    op == DB_LAST) ? NULL : key->data;
	log_db_op(DBATS_LOG_FINEST, fname, line, cw->db, label, logdata,
	    key->size, "");
    }
    int rc = cw->dbc->get(cw->dbc, key, data, flags);
    if (rc != 0)
	log_db_op((rc == DB_NOTFOUND) ? DBATS_LOG_FINE : DBATS_LOG_WARN,
	    fname, line, cw->db, label, key->data, key->size, db_strerror(rc));
    return rc;
}

#define CFG_SET(dh, txn, key, val) \
    raw_db_set(dh->dbConfig, txn, key, strlen(key), &val, sizeof(val), 0)

#define CFG_GET(dh, txn, key, val, flags) \
    raw_db_get(dh->dbConfig, txn, key, strlen(key), &val, sizeof(val), NULL, flags)

/*************************************************************************/

#define bundle_info_key(dh, keybuf, bid) \
    sprintf(keybuf, "bundle%d", bid)

static int read_bundle_info(dbats_handler *dh, dbats_snapshot *ds, int bid,
    uint32_t flags)
{
    uint8_t key = bid;
    DB_TXN *txn = ds ? ds->txn : dh->txn;
    return raw_db_get(dh->dbBundle, txn, &key, sizeof(key), &dh->bundle[bid],
	sizeof(dh->bundle[bid]), NULL, flags);
}

static int write_bundle_info(dbats_handler *dh, dbats_snapshot *ds, int bid)
{
    uint8_t key = bid;
    DB_TXN *txn = ds ? ds->txn : dh->txn;
    return raw_db_set(dh->dbBundle, txn, &key, sizeof(key), &dh->bundle[bid],
	sizeof(dh->bundle[bid]), 0);
}

/*************************************************************************/

static int make_file_writable(const char *filename)
{
    int fd;
    struct stat statbuf;

    fd = open(filename, 0, 0);
    if (fd < 0) {
	if (errno == ENOENT) return errno;
	dbats_log(DBATS_LOG_ERR, "Error opening %s: %s", filename, strerror(errno));
	return errno;
    }
    if (fstat(fd, &statbuf) < 0) {
	dbats_log(DBATS_LOG_ERR, "fstat %s: %s", filename, strerror(errno));
	return errno;
    }
    mode_t newmode = statbuf.st_mode;
    if (newmode & S_IRUSR) newmode |= S_IWUSR;
    if (newmode & S_IRGRP) newmode |= S_IWGRP;
    if (newmode & S_IROTH) newmode |= S_IWOTH;
    if (newmode == statbuf.st_mode) return 0; // no change
    dbats_log(DBATS_LOG_INFO, "%s mode %03o -> %03o", filename, statbuf.st_mode, newmode);
#if HAVE_FCHMOD
    if (fchmod(fd, newmode) < 0)
#else
    if (chmod(filename, newmode) < 0)
#endif
    {
	dbats_log(DBATS_LOG_ERR, "chmod %s: %s", filename, strerror(errno));
	return errno;
    }
    return 0;
}

static void error_callback(const DB_ENV *dbenv, const char *errpfx, const char *msg)
{
    if (errpfx)
	dbats_log(DBATS_LOG_ERR, "%s: %s", errpfx, msg);
    else
	dbats_log(DBATS_LOG_ERR, "%s", msg);
}

static void message_callback(const DB_ENV *dbenv, const char *msg)
{
    dbats_log(DBATS_LOG_FINE, "%s", msg);
}

// Warning: never open the same dbats more than once in the same process,
// because there must be only one DB_ENV per environment per process when
// using DB_REGISTER.
// http://docs.oracle.com/cd/E17076_02/html/programmer_reference/transapp_app.html
int dbats_open(dbats_handler **dhp,
    const char *path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags,
    int mode)
{
    int major, minor, patch;
    char *version = db_version(&major, &minor, &patch);
    dbats_log(DBATS_LOG_CONFIG, "db_version: '%s'", version);
    if (major != DB_VERSION_MAJOR || minor != DB_VERSION_MINOR || patch != DB_VERSION_PATCH) {
	dbats_log(DBATS_LOG_ERR, "BDB version mismatch: libdb %d.%d.%d != db.h %d.%d.%d",
	    major, minor, patch, DB_VERSION_MAJOR, DB_VERSION_MINOR, DB_VERSION_PATCH);
	return DB_VERSION_MISMATCH;
    }

    if (!HAVE_DB_SET_LK_EXCLUSIVE && (flags & DBATS_EXCLUSIVE)) {
	dbats_log(DBATS_LOG_ERR,
	    "Exclusive open is not supported in this version of BDB.");
	return EINVAL;
    }

    int rc;
    uint32_t dbflags = DB_THREAD | DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG |
	DB_INIT_TXN | DB_REGISTER;
    int is_new = 0;

    dbats_handler *dh = ecalloc(1, sizeof(dbats_handler), "dh");
    if (!dh) return ENOMEM;
    dh->cfg.readonly = !!(flags & DBATS_READONLY);
    dh->cfg.updatable = !!(flags & DBATS_UPDATABLE);
    dh->cfg.compress = !(flags & DBATS_UNCOMPRESSED);
    dh->cfg.exclusive = !!(flags & DBATS_EXCLUSIVE);
    dh->cfg.no_txn = !!(flags & DBATS_NO_TXN);
    dh->cfg.values_per_entry = 1;
    dh->serialize = 1;
    dh->path = path;

    if (!dh->cfg.readonly) {
	dbflags |= DB_RECOVER | DB_CREATE;
    }

    if ((rc = db_env_create(&dh->dbenv, 0)) != 0) {
	dbats_log(DBATS_LOG_ERR, "Error creating DB env: %s",
	    db_strerror(rc));
	return rc;
    }

    dh->dbenv->set_errcall(dh->dbenv, error_callback);
    dh->dbenv->set_errpfx(dh->dbenv, dh->path);

    dh->dbenv->set_msgcall(dh->dbenv, message_callback);
    //dh->dbenv->set_verbose(dh->dbenv, DB_VERB_FILEOPS, 1);
    dh->dbenv->set_verbose(dh->dbenv, DB_VERB_RECOVERY, 1);
    dh->dbenv->set_verbose(dh->dbenv, DB_VERB_REGISTER, 1);

    if ((flags & DBATS_NO_TXN) && (flags & DBATS_EXCLUSIVE)) {
	dbats_log(DBATS_LOG_ERR,
	    "DBATS_EXCLUSIVE and DBATS_NO_TXN are not compatible");
	return EINVAL;
    }

    if (flags & DBATS_CREATE) {
	if (dh->cfg.readonly) {
	    dbats_log(DBATS_LOG_ERR,
		"DBATS_CREATE and DBATS_READONLY are not compatible");
	    return EINVAL;
	} else if (mkdir(path, 0777) == 0) {
	    // new database dir
	    is_new = 1;
	    dbflags |= DB_CREATE;
	} else if (errno == EEXIST) {
	    // existing database dir
	    flags &= ~DBATS_CREATE;
	    // Wait for "config" database to exist to avoid race condition with
	    // another process creating a dbats environment.
	    char filename[PATH_MAX];
	    struct stat statbuf;
	    int tries_limit = 30;
	    sprintf(filename, "%s/dbdata/config", path);
	    while (stat(filename, &statbuf) != 0) {
		if (errno != ENOENT) {
		    dbats_log(DBATS_LOG_ERR, "Error checking %s: %s",
			filename, strerror(errno));
		    return errno;
		}
		if (--tries_limit <= 0) {
		    dbats_log(DBATS_LOG_ERR, "Retry limit exceeded waiting for %s",
			filename);
		    return errno;
		}
		sleep(1);
	    }
	} else {
	    // new database dir failed
	    dbats_log(DBATS_LOG_ERR, "Error creating %s: %s",
		path, strerror(errno));
	    return errno;
	}
    }

    if (is_new) {
	char filename[PATH_MAX];

	sprintf(filename, "%s/dbdata", path);
	if (mkdir(filename, 0777) != 0) {
	    dbats_log(DBATS_LOG_ERR, "Error creating %s: %s",
		filename, strerror(errno));
	    return errno;
	}

	sprintf(filename, "%s/dblog", path);
	if (mkdir(filename, 0777) != 0) {
	    dbats_log(DBATS_LOG_ERR, "Error creating %s: %s",
		filename, strerror(errno));
	    return errno;
	}

	sprintf(filename, "%s/dbmeta", path);
	if (mkdir(filename, 0777) != 0) {
	    dbats_log(DBATS_LOG_ERR, "Error creating %s: %s",
		filename, strerror(errno));
	    return errno;
	}
	rc = dh->dbenv->set_metadata_dir(dh->dbenv, filename);
	if (rc != 0) {
	    dbats_log(DBATS_LOG_ERR, "set_metadata_dir %s: %s",
		filename, db_strerror(rc));
	    return rc;
	}

	sprintf(filename, "%s/DB_CONFIG", path);
	FILE *file = fopen(filename, "w");
	if (!file) {
	    dbats_log(DBATS_LOG_ERR, "Error opening %s: %s",
		filename, strerror(errno));
	    return errno;
	}

	fprintf(file, "add_data_dir dbdata\n"); // where to find data
	fprintf(file, "set_create_dir dbdata\n"); // where to create data
	fprintf(file, "set_lg_dir dblog\n");
	fprintf(file, "set_metadata_dir dbmeta\n");

	// XXX These should be based on max expected number of metric keys?
	// 40000 is enough to insert at least 2000000 metric keys.
	fprintf(file, "set_lk_max_locks 40000\n");
	fprintf(file, "set_lk_max_objects 40000\n");

	fprintf(file, "set_lk_detect DB_LOCK_YOUNGEST\n");
	fprintf(file, "set_lg_bsize 1048576\n"); // default is ~32K

	fclose(file);
    }

    if ((rc = dh->dbenv->open(dh->dbenv, path, dbflags, mode)) != 0) {
	dbats_log(DBATS_LOG_ERR, "Error opening DB %s: %s", path, db_strerror(rc));
	return rc;
    }

    const char *dirname;
    const char **dirnames;
    dh->dbenv->get_data_dirs(dh->dbenv, &dirnames);
    for ( ; *dirnames; dirnames++)
	dbats_log(DBATS_LOG_FINE, "data dir: %s", *dirnames);
    dh->dbenv->get_create_dir(dh->dbenv, &dirname);
    dbats_log(DBATS_LOG_FINE, "create-data dir: %s", dirname);
    dh->dbenv->get_lg_dir(dh->dbenv, &dirname);
    dbats_log(DBATS_LOG_FINE, "lg dir: %s", dirname);
    dh->dbenv->get_metadata_dir(dh->dbenv, &dirname);
    dbats_log(DBATS_LOG_FINE, "metadata dir: %s", dirname);

#if defined(DB_LOG_AUTO_REMOVE)
    dh->dbenv->log_set_config(dh->dbenv, DB_LOG_AUTO_REMOVE, 1);
#elif defined(DB_LOG_AUTOREMOVE)
    dh->dbenv->set_flags(dh->dbenv, DB_LOG_AUTOREMOVE, 1);
#else
# error "no DB_LOG_AUTO_REMOVE option"
#endif

    if ((rc = begin_transaction(dh, NULL, &dh->txn, "open txn")) != 0)
	goto abort;

#define open_db(db,name,type,use_txn) raw_db_open(dh, &dh->db, name, type, flags, mode, use_txn)
    if ((rc = open_db(dbConfig, "config", DB_BTREE, 1)) != 0) goto abort;

    dh->cfg.entry_size = sizeof(dbats_value); // for db_get_buf_len

#define initcfg(field, key, defaultval) \
    do { \
	if (is_new) { \
	    dh->cfg.field = (defaultval); \
	    if ((rc = CFG_SET(dh, dh->txn, key, dh->cfg.field)) != 0) \
		goto abort; \
	} else { \
	    int writelock = !dh->cfg.readonly; \
	    if ((rc = CFG_GET(dh, dh->txn, key, dh->cfg.field, writelock ? DB_RMW : 0)) != 0) {\
		dbats_log(DBATS_LOG_ERR, "%s: missing config: %s", path, key); \
		goto abort; \
	    } \
	} \
	dbats_log(DBATS_LOG_CONFIG, "cfg: %s = %u", key, dh->cfg.field); \
    } while (0)

    initcfg(version,          "version",           DBATS_DB_VERSION);
    initcfg(period,           "period",            period);
    initcfg(values_per_entry, "values_per_entry",  values_per_entry);

    if (dh->cfg.version > DBATS_DB_VERSION) {
	dbats_log(DBATS_LOG_ERR, "database version %d > library version %d",
	    dh->cfg.version, DBATS_DB_VERSION);
	return DB_VERSION_MISMATCH;
    }
    if (dh->cfg.version < 8) {
	dbats_log(DBATS_LOG_ERR, "obsolete database version %d", dh->cfg.version);
	return DB_VERSION_MISMATCH;
    }

    if ((rc = open_db(dbBundle,  "bundle",  DB_BTREE, 1) != 0)) goto abort;
    if ((rc = open_db(dbKeytree, "keytree", DB_BTREE, 1) != 0)) goto abort;
    if ((rc = open_db(dbKeyid,   "keyid",   DB_RECNO, 1) != 0)) goto abort;
    if ((rc = open_db(dbValues,  "values",  DB_BTREE, 1) != 0)) goto abort;
    if ((rc = open_db(dbIsSet,   "is_set",  DB_BTREE, 1) != 0)) goto abort;
    if ((rc = open_db(dbSequence,"sequence",DB_BTREE, 1) != 0)) goto abort;
#undef open_db
#undef initcfg

    dh->bundle = emalloc(MAX_NUM_BUNDLES * sizeof(*dh->bundle), "dh->bundle");
    if (!dh->bundle) { rc = ENOMEM; goto abort; }

    dh->cfg.entry_size = dh->cfg.values_per_entry * sizeof(dbats_value);

    int swapped;
    dh->dbValues->get_byteswapped(dh->dbValues, &swapped);
    dh->swapped = swapped;

    if (is_new) {
	// initialize metadata for time series bundles
	memset(&dh->bundle[0], 0, sizeof(dbats_bundle_info));
	dh->bundle[0].func = DBATS_AGG_DATA;
	dh->bundle[0].steps = 1;
	dh->bundle[0].period = dh->cfg.period;
	dh->cfg.num_bundles = 1;
	rc = write_bundle_info(dh, NULL, 0);
	if (rc != 0) goto abort;

	// initialize other metadata
	uint32_t min_keep_time = 0;
	rc = CFG_SET(dh, dh->txn, "min_keep_time", min_keep_time);
	if (rc != 0) goto abort;

	// initialize keytree_seq
	DB_SEQUENCE *ktseq;
	rc = db_sequence_create(&ktseq, dh->dbSequence, 0);
	if (rc != 0) goto abort;
	rc = ktseq->initial_value(ktseq, 1);
	if (rc != 0) goto abort;
	rc = ktseq->set_range(ktseq, 1, KTKEY_MAX_NODE);
	if (rc != 0) goto abort;
	DBT_in(dbt_seq, (void*)"keytree_seq", strlen("keytree_seq"));
	rc = ktseq->open(ktseq, NULL, &dbt_seq, DB_CREATE);
	if (rc != 0) goto abort;
	ktseq->close(ktseq, 0);

    } else {
	// load metadata for time series bundles
	for (int bid = 0; ; bid++) {
	    rc = read_bundle_info(dh, NULL, bid, 0);
	    if (rc != 0) {
		if (rc == DB_NOTFOUND && bid > 0) break; // no more bundles
		goto abort;
	    }
	    dh->cfg.num_bundles = bid + 1;
	}
    }

    if (!dh->cfg.readonly) {
	// If we want other users to be able to read the db, we have to give
	// them write permission on the environment's "__db.###" shared memory
	// region files and "__db.register" file, whenever these files are
	// created (i.e., when creating a new DB, or recovering a corrupt DB).
	char filename[PATH_MAX];
	for (int i = 1; ; i++) {
	    sprintf(filename, "%s/__db.%03d", path, i);
	    if (make_file_writable(filename) != 0) break;
	}
	sprintf(filename, "%s/__db.register", path);
	make_file_writable(filename);
    }

    dh->is_open = 1;
    *dhp = dh;
    return 0;

abort:
    dbats_abort_open(dh);
    return rc;
}

int dbats_aggregate(dbats_handler *dh, enum dbats_agg_func func, int steps)
{
    int rc;

    if (dh->cfg.readonly)
	return EPERM;

    if (func <= DBATS_AGG_DATA || func >= DBATS_AGG_N)
	return EINVAL;

    // We don't need this value, but it locks against other threads/processes
    // calling dbats_select_snap() or dbats_aggregate().
    uint32_t dummy;
    rc = CFG_GET(dh, dh->txn, "min_keep_time", dummy, DB_RMW);
    if (rc != 0) return rc;

    DB_BTREE_STAT *stats;
    rc = dh->dbIsSet->stat(dh->dbIsSet, dh->txn, &stats, DB_FAST_STAT);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "Error getting is_set stats: %s", db_strerror(rc));
	return rc;
    }
    int empty = (stats->bt_pagecnt > stats->bt_empty_pg);
    free(stats);

    if (!empty) {
	dbats_log(DBATS_LOG_ERR,
	    "Adding a new aggregation to existing data is not yet supported.");
	return EINVAL;
    }

    if (dh->cfg.num_bundles >= MAX_NUM_BUNDLES) {
	dbats_log(DBATS_LOG_ERR, "Too many bundles (%d)", MAX_NUM_BUNDLES);
	return ENOMEM;
    }

    int bid = dh->cfg.num_bundles;

    memset(&dh->bundle[bid], 0, sizeof(dbats_bundle_info));
    dh->bundle[bid].func = func;
    dh->bundle[bid].steps = steps;
    dh->bundle[bid].period = dh->bundle[0].period * steps;

    rc = write_bundle_info(dh, NULL, bid);
    if (rc != 0) return rc;

    dh->cfg.num_bundles++;
    return 0;
}

enum dbats_agg_func dbats_find_agg_func(const char *name)
{
    enum dbats_agg_func func;
    for (func = DBATS_AGG_DATA + 1; func < DBATS_AGG_N; func++) {
	if (strcmp(dbats_agg_func_label[func], name) == 0)
	    return func;
    }
    return DBATS_AGG_NONE;
}

int dbats_get_start_time(dbats_handler *dh, dbats_snapshot *ds, int bid, uint32_t *start)
{
    int rc;
    cursor_wrapper cw;

    *start = 0;
    DB_TXN *txn = ds ? ds->txn : dh->txn;
    rc = raw_cursor_open(dh->dbValues, txn, &cw, 0);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "dbats_get_start_time %d: cursor: %s", bid, db_strerror(rc));
	return rc;
    }
    fragkey_t dbkey = { htonl(bid), htonl(0), htonl(0) };
    DBT_in(dbt_key, &dbkey, sizeof(dbkey));
    dbt_key.ulen = sizeof(dbkey);
    DBT_null(dbt_data);
    rc = raw_cursor_get(&cw, &dbt_key, &dbt_data, DB_SET_RANGE | DB_READ_COMMITTED);
    if (rc == DB_NOTFOUND) {
	// do nothing
    } else if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "dbats_get_start_time %d: get: %s", bid, db_strerror(rc));
    } else { // success
	*start = ntohl(dbkey.time);
    }

    raw_cursor_close(&cw);
    return rc;
}

int dbats_get_end_time(dbats_handler *dh, dbats_snapshot *ds, int bid, uint32_t *end)
{
    int rc;
    cursor_wrapper cw;

    *end = 0;
    DB_TXN *txn = ds ? ds->txn : dh->txn;
    rc = raw_cursor_open(dh->dbValues, txn, &cw, 0);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "dbats_get_end_time %d: cursor: %s", bid, db_strerror(rc));
	return rc;
    }
    fragkey_t dbkey = { htonl(bid), htonl(UINT32_MAX), htonl(0) };
    DBT_in(dbt_key, &dbkey, sizeof(dbkey));
    dbt_key.ulen = sizeof(dbkey);
    DBT_null(dbt_data);
    rc = raw_cursor_get(&cw, &dbt_key, &dbt_data, DB_SET_RANGE | DB_READ_COMMITTED);

    if (rc == 0) {
	rc = raw_cursor_get(&cw, &dbt_key, &dbt_data, DB_PREV | DB_READ_COMMITTED);
    } else if (rc == DB_NOTFOUND) {
	rc = raw_cursor_get(&cw, &dbt_key, &dbt_data, DB_LAST | DB_READ_COMMITTED);
    } else {
	dbats_log(DBATS_LOG_ERR, "dbats_get_end_time %d: get: %s", bid, db_strerror(rc));
	raw_cursor_close(&cw);
	return rc;
    }

    if (rc == DB_NOTFOUND) {
	// do nothing
    } else if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "dbats_get_end_time %d: prev: %s", bid, db_strerror(rc));
    } else if (ntohl(dbkey.bid) != bid) {
	rc = DB_NOTFOUND;
    } else { // success
	*end = ntohl(dbkey.time);
    }

    raw_cursor_close(&cw);
    return rc;
}

int dbats_best_bundle(dbats_handler *dh, enum dbats_agg_func func,
    uint32_t start, uint32_t end, int max_points)
{
    typedef struct {
	int bid;
	uint32_t start;    // start time of overlap between request and bundle
	uint32_t npoints;  // # of points in bundle within time range
    } info;

    info best = { -1, UINT32_MAX, UINT32_MAX };
    info current;
    uint32_t max_end;

    dbats_get_end_time(dh, NULL, 0, &max_end);
    if (end > max_end) end = max_end;

    for (int bid = 0; bid < dh->cfg.num_bundles; bid++) {
	if (bid != 0 && dh->bundle[bid].func != func) continue;
	current.bid = bid;
	dbats_get_start_time(dh, NULL, bid, &current.start);
	current.start = max(current.start, start);

	{
	    uint32_t nstart = current.start;
	    dbats_normalize_time(dh, bid, &nstart);
	    uint32_t nend = end;
	    dbats_normalize_time(dh, bid, &nend);
	    current.npoints = (nend - nstart) / dh->bundle[bid].period + 1;
	}

	if (current.start != best.start) {
	    // pick the one that better covers the time range
	    if (current.start < best.start) best = current;
	    continue;
	}

	if (max_points > 0 && (best.npoints > max_points || current.npoints > max_points)) {
	    // one or both have too many points; pick the one with fewer points
	    if (current.npoints < best.npoints) best = current;
	} else {
	    // neither has too many points; pick the one with more points
	    if (current.npoints > best.npoints) best = current;
	}
    }
    return best.bid;
}

// "DELETE FROM db WHERE db.bid = bid AND db.time >= start AND db.time < end"
static int delete_frags(DB_TXN *txn, DB *db, int bid,
    uint32_t start, uint32_t end)
{
    const char *name;
    db->get_dbname(db, &name, NULL);

    dbats_log(DBATS_LOG_FINE, "delete_frags %s %d [%u,%u)", name,
	bid, start, end);
    int rc;
    cursor_wrapper cw;
    rc = raw_cursor_open(db, txn, &cw, 0);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "delete_frags %s %d [%u,%u): cursor: %s", name,
	    bid, start, end, db_strerror(rc));
	return rc;
    }
    fragkey_t dbkey = { htonl(bid), htonl(start), htonl(0) };
    DBT_in(dbt_key, &dbkey, sizeof(dbkey));
    dbt_key.ulen = sizeof(dbkey);
    DBT_null(dbt_data);
    // Range cursor search depends on dbkey being {bid, time, frag_id}, in
    // that order, with big endian values.
    rc = raw_cursor_get(&cw, &dbt_key, &dbt_data, DB_SET_RANGE);
    while (1) {
	if (rc == DB_NOTFOUND) break;
	if (rc != 0) {
	    dbats_log(DBATS_LOG_ERR, "delete_frags %s %d %u %d: get: %s", name,
		ntohl(dbkey.bid), ntohl(dbkey.time), ntohl(dbkey.frag_id),
		db_strerror(rc));
	    return rc;
	}
	if (ntohl(dbkey.bid) != bid || ntohl(dbkey.time) >= end) break;
	rc = cw.dbc->del(cw.dbc, 0);
	if (rc != 0) {
	    dbats_log(DBATS_LOG_ERR, "delete_frags %s %d %u %d: del: %s", name,
		ntohl(dbkey.bid), ntohl(dbkey.time), ntohl(dbkey.frag_id),
		db_strerror(rc));
	    return rc;
	} else {
	    dbats_log(DBATS_LOG_FINE, "delete_frags %d %u %d",
		ntohl(dbkey.bid), ntohl(dbkey.time), ntohl(dbkey.frag_id));
	}
	rc = raw_cursor_get(&cw, &dbt_key, &dbt_data, DB_NEXT);
    }
    rc = raw_cursor_close(&cw);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "delete_frags %s %d [%u,%u): close: %s", name,
	    bid, start, end, db_strerror(rc));
	return rc;
    }
    return 0;
}

// Delete values earlier than the keep limit
static int truncate_bundle(dbats_handler *dh, dbats_snapshot *ds, int bid)
{
    dbats_log(DBATS_LOG_FINE, "truncate_bundle %d", bid);
    int rc;
    DB_TXN *parent = ds ? ds->txn : dh->txn;

    dbats_bundle_info *bundle = &dh->bundle[bid];
    if (bundle->keep == 0) return 0; // keep all

    uint32_t start, end;
    dbats_get_start_time(dh, ds, bid, &start);
    dbats_get_end_time(dh, ds, bid, &end);
    uint32_t keep_period = (bundle->keep-1) * bundle->period;
    uint32_t new_start = (keep_period > end) ? 0 : (end - keep_period);

    if (bid == 0 && ds->min_keep_time < new_start) {
	ds->min_keep_time = new_start;
	rc = CFG_SET(dh, parent, "min_keep_time", ds->min_keep_time);
	if (rc != 0) return rc;
    }
    if (start >= new_start) return 0; // there's nothing to truncate

    DB_TXN *txn;
    rc = begin_transaction(dh, parent, &txn, "truncate txn");
    if (rc != 0) return rc;
    rc = delete_frags(txn, dh->dbValues, bid, 0, new_start);
    if (rc != 0) goto abort;
    rc = delete_frags(txn, dh->dbIsSet, bid, 0, new_start);
    if (rc != 0) goto abort;
    commit_transaction(dh, txn); // truncate txn
    return 0;

abort:
    // failing to delete frags is just an annoyance, not an error
    abort_transaction(dh, txn); // truncate txn
    return 0;
}

int dbats_series_limit(dbats_handler *dh, int bid, int keep)
{
    int rc = 0;
    DB_TXN *txn;

    rc = begin_transaction(dh, dh->txn, &txn, "keep txn");
    if (rc != 0) return rc;

    rc = read_bundle_info(dh, NULL, bid, DB_RMW);
    if (rc != 0) goto abort;

    if (bid == 0) {
	for (int a = 1; a < dh->cfg.num_bundles; a++) {
	    rc = read_bundle_info(dh, NULL, a, 0);
	    if (rc != 0) goto abort;
	    if (keep > 0 && dh->bundle[a].steps > keep) {
		dbats_log(DBATS_LOG_ERR, "bundle %d requires keeping at least %d "
		    "primary data points", a, dh->bundle[a].steps);
		rc = EINVAL;
		goto abort;
	    }
	}
    }

    dh->bundle[bid].keep = keep;
    rc = truncate_bundle(dh, NULL, bid);
    if (rc != 0) goto abort;

    rc = write_bundle_info(dh, NULL, bid); // write new keep
    if (rc != 0) goto abort;

    return commit_transaction(dh, txn); // "keep txn"

abort:
    abort_transaction(dh, txn); // "keep txn"
    return rc;
}

void *dbats_get_userdata(dbats_handler *dh)
{
    return dh->userdata;
}

void dbats_set_userdata(dbats_handler *dh, void *data)
{
    dh->userdata = data;
}

/*************************************************************************/

// Free contents of in-memory tslice.
static int clear_tslice(dbats_snapshot *ds, int bid)
{
    dbats_tslice *tslice = ds->tslice[bid];
    dbats_log(DBATS_LOG_FINE, "Free tslice t=%u bid=%d", tslice->time, bid);

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
static int flush_tslice(dbats_snapshot *ds, int bid)
{
    dbats_log(DBATS_LOG_FINE, "Flush t=%u bid=%d", ds->tslice[bid]->time, bid);
    size_t frag_size = fragsize(ds->dh);
    char *buf = NULL;
    int rc;

    // Write fragments to the DB
    fragkey_t dbkey = { htonl(bid), htonl(ds->tslice[bid]->time), 0};
    for (int f = 0; f < ds->tslice[bid]->num_frags; f++) {
	if (!ds->tslice[bid]->frag[f]) continue;
	dbkey.frag_id = htonl(f);
	if (!ds->tslice[bid]->frag_changed[f]) {
	    dbats_log(DBATS_LOG_VFINE, "Skipping frag %d (unchanged)", f);
	} else {
	    if (ds->dh->cfg.compress) {
		if (!ds->state_compress) {
		    ds->state_compress = ecalloc(1, sizeof(qlz_state_compress),
			"state_compress");
		    if (!ds->state_compress) return ENOMEM;
		}
		if (!buf) {
		    buf = (char*)emalloc(1 + frag_size + QLZ_OVERHEAD,
			"compression buffer");
		    if (!buf) return errno ? errno : ENOMEM;
		}
		buf[0] = 1; // compression flag
		ds->tslice[bid]->frag[f]->compressed = 1;
		size_t compress_len = qlz_compress(ds->tslice[bid]->frag[f],
		    buf+1, frag_size, ds->state_compress);
		dbats_log(DBATS_LOG_VFINE, "Frag %d compression: %zu -> %zu "
		    "(%.1f %%)", f, frag_size, compress_len,
		    compress_len*100.0/frag_size);
		rc = raw_db_set(ds->dh->dbValues, ds->txn, &dbkey, sizeof(dbkey),
		    buf, compress_len + 1, 0);
		if (rc != 0) return rc;
	    } else {
		ds->tslice[bid]->frag[f]->compressed = 0;
		dbats_log(DBATS_LOG_VFINE, "Frag %d write: %zu", f, frag_size);
		rc = raw_db_set(ds->dh->dbValues, ds->txn, &dbkey, sizeof(dbkey),
		    ds->tslice[bid]->frag[f], frag_size, 0);
		if (rc != 0) return rc;
	    }
	    ds->tslice[bid]->frag_changed[f] = 0;

	    uint32_t ones = 0; // # of leading "1" bits
	    int zeroes = 0; // have any "0"s been seen?
	    int matches = 1; // bit string matches regexp "^1*0*$"?
	    for (int i = 0; i < vec_size(ENTRIES_PER_FRAG); i++) {
		uint8_t byte = ds->tslice[bid]->is_set[f][i];
		if (zeroes) { if (!(matches = !byte)) break; }
		else if (byte == 0xFF) { ones += 8; }
		else if (byte == 0x7F) { ones += 7; zeroes = 1; }
		else if (byte == 0x3F) { ones += 6; zeroes = 1; }
		else if (byte == 0x1F) { ones += 5; zeroes = 1; }
		else if (byte == 0x0F) { ones += 4; zeroes = 1; }
		else if (byte == 0x07) { ones += 3; zeroes = 1; }
		else if (byte == 0x03) { ones += 2; zeroes = 1; }
		else if (byte == 0x01) { ones += 1; zeroes = 1; }
		else if (byte == 0x00) { ones += 0; zeroes = 1; }
		else { matches = 0; break; }
	    }
	    if (matches) {
		// write number of leading "1" bits
		ones = htonl(ones);
		rc = raw_db_set(ds->dh->dbIsSet, ds->txn, &dbkey, sizeof(dbkey),
		    &ones, sizeof(ones), 0);
	    } else {
		// write the whole is_set vector
		rc = raw_db_set(ds->dh->dbIsSet, ds->txn, &dbkey, sizeof(dbkey),
		    ds->tslice[bid]->is_set[f], vec_size(ENTRIES_PER_FRAG), 0);
	    }
	    if (rc != 0) return rc;
	}
    }
    if (buf) free(buf);

    rc = truncate_bundle(ds->dh, ds, bid);
    if (rc != 0) return rc;

    return 0;
}

/*************************************************************************/

static void free_isset(dbats_snapshot *ds)
{
    if (ds->is_set) {
	int tn = (ds->active_end - ds->active_start) / ds->dh->bundle[0].period + 1;
	for (int ti = 0; ti < tn; ti++) {
	    if (ds->is_set[ti]) {
		for (int f = 0; f < ds->tslice[0]->num_frags; f++) {
		    if (ds->is_set[ti][f]) {
			if (ds->tslice[0]->is_set[f] != ds->is_set[ti][f])
			    free(ds->is_set[ti][f]); // shared; don't free twice
			ds->is_set[ti][f] = 0;
		    }
		}
		free(ds->is_set[ti]);
		ds->is_set[ti] = 0;
	    }
	}
	free(ds->is_set);
	ds->is_set = 0;
    }
}

int dbats_commit_open(dbats_handler *dh)
{
    dbats_log(DBATS_LOG_FINE, "dbats_commit_open");
    int rc = commit_transaction(dh, dh->txn);
    dh->txn = NULL;
    return rc;
}

int dbats_abort_open(dbats_handler *dh)
{
    dbats_log(DBATS_LOG_FINE, "dbats_abort_open");
    int rc = abort_transaction(dh, dh->txn);
    dh->txn = NULL;
    return rc;
}

static void free_snapshot(dbats_snapshot *ds)
{
    for (int bid = 0; bid < ds->num_bundles; bid++) {
	clear_tslice(ds, bid);
	free(ds->tslice[bid]);
	// ds->tslice[bid] = NULL;
    }

    if (ds->state_decompress)
	free(ds->state_decompress);
    if (ds->state_compress)
	free(ds->state_compress);
    if (ds->db_get_buf)
	free(ds->db_get_buf);
    if (ds->valbuf)
	free(ds->valbuf);

    ds->dh = NULL;
    free(ds);
}

#if 0
static void recycle_snapshot(dbats_snapshot *ds)
{
    ds->changed = 0;
    // XXX don't clear or free tslice
    // XXX store ds in dh->snapshot_pool (thread-safely)
}
#endif

int dbats_commit_snap(dbats_snapshot *ds)
{
    int rc = 0;
    dbats_log(DBATS_LOG_FINE, "dbats_commit_snap %u", ds->tslice[0]->time);

    free_isset(ds);

    if (!ds->dh->serialize) {
	// In dbats_select_snap(), we read min_keep_time with DB_READ_COMMITTED;
	// it may have changed since then.
	rc = CFG_GET(ds->dh, ds->txn, "min_keep_time", ds->min_keep_time, DB_RMW);
	if (rc != 0) {
	    dbats_log(DBATS_LOG_ERR, "error getting %s: %s", "min_keep_time",
		db_strerror(rc));
	    goto end;
	}

	for (int bid = 0; bid < ds->num_bundles; bid++) {
	    if (ds->changed && ds->tslice[bid]->time < ds->min_keep_time) {
		dbats_log(DBATS_LOG_ERR, "dbats_commit %u: illegal attempt to set "
		    "value in bundle %d at time %u before series limit %u",
		    ds->tslice[bid]->time, bid, ds->tslice[bid]->time,
		    ds->min_keep_time);
		rc = EINVAL;
		goto end;
	    }
	}
    }

end:
    for (int bid = 0; bid < ds->num_bundles; bid++) {
	if (ds->changed && rc == 0) {
	    rc = flush_tslice(ds, bid);
	}
    }

    if (rc == 0)
	rc = commit_transaction(ds->dh, ds->txn);
    else
	abort_transaction(ds->dh, ds->txn);

    ds->changed = 0;

#if 0
    if (ds->dh->cfg.exclusive && ds->dh->is_open && rc == 0)
	recycle_snapshot(ds);
    else
#endif
	free_snapshot(ds);

    return rc;
}

int dbats_abort_snap(dbats_snapshot *ds)
{
    dbats_log(DBATS_LOG_FINE, "dbats_abort_snap %u", ds->tslice[0]->time);

    free_isset(ds);

    int rc = abort_transaction(ds->dh, ds->txn);
    free_snapshot(ds);
    return rc;
}

/*************************************************************************/

int dbats_close(dbats_handler *dh)
{
    dbats_log(DBATS_LOG_FINE, "dbats_close %s", dh->path);
    int rc = 0;
    int myrc = 0;
    if (!dh->is_open)
	return EINVAL;
    dh->is_open = 0;

    if (dh->txn) {
	dbats_log(DBATS_LOG_WARN, "aborting uncommitted dbats_open()");
	abort_transaction(dh, dh->txn);
	dh->txn = NULL;
    }

    if (dh->bundle) free(dh->bundle);

    // BDB v4 requires explicit close of each db
    dh->dbConfig->close(dh->dbConfig, 0);
    dh->dbBundle->close(dh->dbBundle, 0);
    dh->dbKeytree->close(dh->dbKeytree, 0);
    dh->dbKeyid->close(dh->dbKeyid, 0);
    dh->dbValues->close(dh->dbValues, 0);
    dh->dbIsSet->close(dh->dbIsSet, 0);
    dh->dbSequence->close(dh->dbSequence, 0);

    rc = dh->dbenv->close(dh->dbenv, 0);
    if (myrc == 0 && rc != 0) myrc = rc;

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

static int load_isset(dbats_snapshot *ds, fragkey_t *dbkey, uint8_t **dest,
    uint32_t flags)
{
    uint8_t *buf = NULL;
    int rc;
    uint32_t value_len;

    if (!(buf = emalloc(vec_size(ENTRIES_PER_FRAG), "is_set")))
	return errno ? errno : ENOMEM; // error

    rc = raw_db_get(ds->dh->dbIsSet, ds->txn, dbkey, sizeof(*dbkey),
	buf, vec_size(ENTRIES_PER_FRAG), &value_len, flags);

    if (rc == DB_NOTFOUND) {
	if (ntohl(dbkey->time) == ds->tslice[ntohl(dbkey->bid)]->time) {
	    memset(buf, 0, vec_size(ENTRIES_PER_FRAG));
	    *dest = buf;
	} else {
	    free(buf);
	    *dest = NULL;
	}
	return rc; // no match
    } else if (rc != 0) {
	*dest = NULL;
	return rc; // error
    }

#if ENTRIES_PER_FRAG <= 32
#error ENTRIES_PER_FRAG must be > 32
#endif
    if (value_len == sizeof(uint32_t)) {
	// vector was compressed to a 32-bit counter
	uint32_t bits = ntohl(*((uint32_t*)buf));
	memset(buf, 0xFF, bits/8);
	for (int i = bits - bits%8; i < bits; i++)
	    vec_set(buf, i);
    }

    *dest = buf;

    if (ntohl(dbkey->bid) == 0 && ds->active_last_data < ntohl(dbkey->time))
	ds->active_last_data = ntohl(dbkey->time);

    return 0; // ok
}

static int load_frag(dbats_snapshot *ds, uint32_t t, int bid,
    uint32_t frag_id)
{
    // load fragment
    fragkey_t dbkey = { htonl(bid), htonl(t), htonl(frag_id) };
    int rc;
    uint32_t flags = ds->readonly ? 0 : DB_RMW;

    assert(!ds->tslice[bid]->is_set[frag_id]);
    rc = load_isset(ds, &dbkey, &ds->tslice[bid]->is_set[frag_id], flags);
    if (rc == DB_NOTFOUND) return 0; // no match
    if (rc != 0) return rc; // error

    if (ds->db_get_buf_len < fragsize(ds->dh) + QLZ_OVERHEAD) {
	if (ds->db_get_buf) free(ds->db_get_buf);
	ds->db_get_buf_len = fragsize(ds->dh) + QLZ_OVERHEAD;
	ds->db_get_buf = emalloc(ds->db_get_buf_len, "get buffer");
	if (!ds->db_get_buf) return errno ? errno : ENOMEM;
    }

    uint32_t value_len;
    rc = raw_db_get(ds->dh->dbValues, ds->txn, &dbkey, sizeof(dbkey),
	ds->db_get_buf, ds->db_get_buf_len, &value_len, flags);
    if (rc != 0) {
	if (rc == DB_NOTFOUND) return 0; // no match
	return rc; // error
    }

    int compressed = ((dbats_frag*)ds->db_get_buf)->compressed;
    uint32_t len = !compressed ? fragsize(ds->dh) :
	qlz_size_decompressed((void*)(ds->db_get_buf+1));
    void *ptr = malloc(len);
    if (!ptr) {
	rc = errno;
	dbats_log(DBATS_LOG_ERR, "Can't allocate %u bytes for frag "
	    "t=%u, bid=%u, frag_id=%u", len, t, bid, frag_id);
	return rc; // error
    }

    if (compressed) {
	// decompress fragment
	if (!ds->state_decompress) {
	    ds->state_decompress = ecalloc(1, sizeof(qlz_state_decompress),
		"qlz_state_decompress");
	    if (!ds->state_decompress) return errno ? errno : ENOMEM;
	}
	len = qlz_decompress((void*)(ds->db_get_buf+1), ptr, ds->state_decompress);
	// assert(len == fragsize(ds));
	dbats_log(DBATS_LOG_VFINE, "decompressed frag t=%u, bid=%u, frag_id=%u: "
	    "%u -> %u (%.1f%%)",
	    t, bid, frag_id, value_len-1, len, 100.0*len/(value_len-1));

    } else {
	// copy fragment
	// XXX TODO: don't memcpy; get directly into ptr
	memcpy(ptr, ds->db_get_buf, len);
	dbats_log(DBATS_LOG_VFINE, "copied frag t=%u, bid=%u, frag_id=%u",
	    t, bid, frag_id);
    }

    ds->tslice[bid]->frag[frag_id] = ptr;

    dbats_tslice *tslice = ds->tslice[bid];
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;

    return 0;
}

// Instantiate ds->is_set[*][frag_id]
static int instantiate_isset_frags(dbats_snapshot *ds, int fid)
{
    if (ds->readonly)
	return 0; // ds->is_set is never needed in readonly mode

    int tn = (ds->active_end - ds->active_start) / ds->dh->bundle[0].period + 1;

    if (!ds->is_set) {
	ds->is_set = ecalloc(tn, sizeof(uint8_t**), "ds->is_set");
	if (!ds->is_set) return errno ? errno : ENOMEM;
    }

    uint32_t t = ds->active_start;
    for (int ti = 0; ti < tn; ti++, t += ds->dh->bundle[0].period) {
	fragkey_t dbkey = { 0, htonl(t), 0 };
	if (!ds->is_set[ti]) {
	    ds->is_set[ti] = ecalloc(MAX_NUM_FRAGS, sizeof(uint8_t*),
		"ds->is_set[ti]");
	    if (!ds->is_set[ti]) return errno ? errno : ENOMEM;
	}

	if (t == ds->tslice[0]->time && ds->tslice[0]->is_set[fid]) {
	    assert(!ds->is_set[ti][fid] || ds->is_set[ti][fid] == ds->tslice[0]->is_set[fid]);
	    ds->is_set[ti][fid] = ds->tslice[0]->is_set[fid]; // share
	} else if (!ds->is_set[ti][fid]) {
	    dbkey.frag_id = htonl(fid);
	    assert(!ds->is_set[ti][fid]);
	    int is_current = (t == ds->tslice[0]->time);
	    int rc = load_isset(ds, &dbkey, &ds->is_set[ti][fid],
		is_current ? DB_RMW : 0);
	    if (rc != 0 && rc != DB_NOTFOUND)
		return rc; // error
	    if (is_current) {
		assert(!ds->tslice[0]->is_set[fid]);
		ds->tslice[0]->is_set[fid] = ds->is_set[ti][fid]; // share
	    }
	}
    }
    return 0;
}

int dbats_num_keys(dbats_handler *dh, uint32_t *num_keys)
{
    int rc;
    DB_BTREE_STAT *stats;

    rc = dh->dbKeyid->stat(dh->dbKeyid, dh->txn, &stats, DB_FAST_STAT);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "Error getting keys stats: %s", db_strerror(rc));
	return rc;
    }

    *num_keys = stats->bt_nkeys;
    free(stats);
    return 0;
}

static void set_priority(dbats_snapshot *ds, uint32_t priority)
{
#if HAVE_DB_SET_PRIORITY
    int rc = ds->txn->set_priority(ds->txn, priority);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "set_priority %u: %s", priority, db_strerror(rc));
    } else {
	dbats_log(DBATS_LOG_FINE, "set_priority %u", priority);
    }
#else
    static int warned = 0;
    if (!warned)
	dbats_log(DBATS_LOG_WARN, "can't set_priority with this version of BDB");
    warned = 1;
#endif
}

int dbats_select_snap(dbats_handler *dh, dbats_snapshot **dsp,
    uint32_t time_value, uint32_t flags)
{
    int rc;
    uint32_t min_time = 0;
    uint32_t max_time = 0;
    int tries_limit = 60;

    dbats_log(DBATS_LOG_FINE, "select_snap %u", time_value);
    if (dh->txn) {
	dbats_log(DBATS_LOG_ERR,
	    "dbats_select_snap() is not allowed before dbats_commit_open()");
	return EINPROGRESS;
    }

restart:
    (*dsp) = emalloc(sizeof(dbats_snapshot), "snapshot");
    if (!*dsp) return ENOMEM;

    memset(*dsp, 0, sizeof(**dsp));
    (*dsp)->dh = dh;
    (*dsp)->preload = !!(flags & DBATS_PRELOAD);
    (*dsp)->readonly = dh->cfg.readonly || !!(flags & DBATS_READONLY);
    rc = begin_transaction(dh, dh->txn, &(*dsp)->txn, "snapshot txn");
    if (rc != 0) return rc;

    if (!(*dsp)->readonly) {
	// DB_RMW will block other writers trying to select_snap().
	// DB_READ_COMMITTED won't hold a read lock, so won't block anyone.
	rc = CFG_GET(dh, (*dsp)->txn, "min_keep_time", (*dsp)->min_keep_time,
	    dh->serialize ? DB_RMW : DB_READ_COMMITTED);
	if (rc == DB_LOCK_DEADLOCK) {
	    if (--tries_limit > 0) goto retry;
	    dbats_log(DBATS_LOG_ERR, "select_snap %u: too many deadlocks",
		time_value);
	}
	if (rc != 0) goto abort;
	dbats_log(DBATS_LOG_FINE, "select_snap %u: got %s = %u", time_value,
	    "min_keep_time", (*dsp)->min_keep_time);

	dbats_get_end_time(dh, (*dsp), 0, &(*dsp)->end_time);
    }

    for (int bid = 0; ; bid++) {
	rc = read_bundle_info(dh, *dsp, bid, 0);
	if (rc != 0) {
	    if (rc == DB_NOTFOUND && bid > 0) break; // no more bundles
	    goto abort;
	}
	(*dsp)->num_bundles = bid + 1;
    }

    (*dsp)->tslice = ecalloc((*dsp)->num_bundles, sizeof(*(*dsp)->tslice), "ds->tslice");
    if (!(*dsp)->tslice) {
	rc = ENOMEM;
	goto abort;
    }

    for (int bid = 0; bid < (*dsp)->num_bundles; bid++) {
	uint32_t t = time_value;

	dbats_normalize_time(dh, bid, &t);

	if (!(*dsp)->readonly) {
	    if (t < (*dsp)->min_keep_time) {
		dbats_log(DBATS_LOG_ERR, "select_snap %u: illegal attempt to set "
		    "value in bundle %d at time %u before series limit %u",
		    time_value, bid, t, (*dsp)->min_keep_time);
		rc = EINVAL;
		goto abort;
	    }
	}

	if (!(*dsp)->tslice[bid])
	    (*dsp)->tslice[bid] = ecalloc(1, sizeof(dbats_tslice), "ds->tslice[bid]");
	if (!(*dsp)->tslice[bid]) {
	    rc = ENOMEM;
	    goto abort;
	}
	dbats_tslice *tslice = (*dsp)->tslice[bid];

	if (dh->cfg.exclusive) {
	    if (tslice->time == t) {
		// keep relevant fragments
		dbats_log(DBATS_LOG_FINE, "select_snap %u, bid=%d: already loaded",
		    t, bid);
	    } else {
		// free obsolete fragments
		clear_tslice((*dsp), bid);
	    }
	}

	tslice->time = t;
	if (bid == 0)
	    (*dsp)->active_last_data = t;

	if (!min_time || t < min_time)
	    min_time = t;
	t += dh->bundle[bid].period - dh->bundle[0].period;
	if (t > max_time)
	    max_time = t;
    }
    (*dsp)->active_start = min_time;
    (*dsp)->active_end = max_time;

    if (!(*dsp)->readonly) {
	// Make priority depend on time, so a writer of realtime data always
	// beats a writer of historical data.
	set_priority((*dsp), (*dsp)->tslice[0]->time / dh->bundle[0].period);
    }

    if (!(*dsp)->preload)
	return 0;

    uint32_t num_keys;
    rc = dbats_num_keys(dh, &num_keys);
    if (rc != 0) goto abort;

    for (int bid = 0; bid < (*dsp)->num_bundles; bid++) {
	dbats_tslice *tslice = (*dsp)->tslice[bid];
	uint32_t t = tslice->time;
	int loaded = 0;

	tslice->num_frags = div_ceil(num_keys, ENTRIES_PER_FRAG);
	for (uint32_t frag_id = 0; frag_id < tslice->num_frags; frag_id++) {
	    rc = load_frag((*dsp), t, bid, frag_id);
	    if (rc == DB_LOCK_DEADLOCK) {
		if (--tries_limit > 0) goto retry;
		dbats_log(DBATS_LOG_ERR, "select_snap %u: too many deadlocks",
		    time_value);
	    }
	    if (rc != 0) goto abort;
	    if (tslice->frag[frag_id])
		loaded++;
	}
	dbats_log(DBATS_LOG_FINE, "select_snap %u: bid=%d, loaded %u/%u fragments",
	    time_value, bid, loaded, tslice->num_frags);
    }

    for (int frag_id = 0; frag_id < (*dsp)->tslice[0]->num_frags; frag_id++) {
	rc = instantiate_isset_frags((*dsp), frag_id);
	if (rc == DB_LOCK_DEADLOCK) {
	    if (--tries_limit > 0) goto retry;
	    dbats_log(DBATS_LOG_ERR, "select_snap %u: too many deadlocks",
		time_value);
	}
	if (rc != 0) goto abort;
    }

    return 0;

abort:
    dbats_abort_snap(*dsp); // snapshot txn
    return rc;

retry:
    dbats_log(DBATS_LOG_FINE, "select_snap %u: retry", time_value);
    dbats_abort_snap(*dsp); // snapshot txn
    goto restart;
}


/*************************************************************************/

// like strchr(), except that c may be escaped by preceeding it with '\\'
static char *estrchr(register const char *s, register int c)
{
    while (*s) {
        if (*s == c) return (char *)s;
        if (*s == '\\' && s[1]) s++;
        s++;
    }
    return NULL;
}

// cclass is a pointer to a string of the form "[...]..."
// c is compared against the character class described by cclass.
// If c matches, char_match() returns a pointer to the char after ']' in cclass;
// otherwise, char_match() returns NULL.
static const char *char_match(const char *cclass, int c)
{
    int not = (*++cclass == '^');
    if (not) ++cclass;
    while (1) {
        if (*cclass == ']') return (char*)(not ? cclass + 1 : NULL);
        if (*cclass == '\\') ++cclass;
        if (cclass[1] == '-' && cclass[2] != ']') {
            char low = *cclass;
            cclass += 2;
            if (*cclass == '\\') ++cclass;
            if (c >= low && c <= *cclass) break;
        } else if (c == *cclass) {
	    break;
	}
        ++cclass;
    }
    return not ? NULL : (estrchr(cclass+1, ']') + 1);
}

static int glob_match(const char *pat, const char *str)
{
    while (*pat) {
        switch (*pat) {

        case '\\':
            pat++;
            break;

        case '?':
            if (!*str) return 0;
            str++;
            pat++;
            continue;

        case '*':
            for (++pat; *pat == '*' || *pat == '?'; ++pat) {
                if (*pat == '?' && !*str++) return 0;
            }
            if (!*pat) {
                return 1;
	    } else if (strchr("\\[{", *pat)) {
		// '*' is followed by metachar
                for ( ; *str; str++)
		    if (glob_match(pat, str)) return 1;
                return 0;
            }
	    // optimization: scan for *pat before recursive function call
	    for ( ; *str; str++)
		if (*str == *pat && glob_match(pat+1, str+1))
		    return 1;
	    return 0;

        case '[':
            if (!(pat = char_match(pat, *str++))) return 0;
            continue;

	case '{':
	    ++pat;
	    {
		int failed = 0;
		const char *strstart = str;
		while (1) {
		    if (*pat == '\\') {
			++pat;
		    } else if (*pat == ',') {
			if (failed) {
			    str = strstart;
			    failed = 0;
			    ++pat;
			    continue;
			}
			pat = estrchr(pat, '}') + 1;
			break; // success
		    } else if (*pat == '}') {
			if (failed) return 0;
			++pat;
			break; // success
		    }
		    if (*pat++ != *str++)
			failed = 1;
		}
	    }
	    continue;
	}

	if (*pat++ != *str++) return 0;
    }
    return *pat == *str;
}

// verify syntax of glob pattern
static int glob_validate(const char *pat)
{
    while (*pat) {
        switch (*pat) {
        case '\\':
            if (*++pat) pat++;
            break;
        case '[':
            if (!(pat = estrchr(pat, ']'))) {
                dbats_log(DBATS_LOG_ERR, "glob error: unmatched '['");
                return 0;
            }
            pat++;
            break;
        case '{':
            if (!(pat = estrchr(pat, '}'))) {
                dbats_log(DBATS_LOG_ERR, "glob error: unmatched '{'");
                return 0;
            }
            pat++;
            break;
        case '?':
        case '*':
        default:
            pat++;
            break;
        }
    }
    return 1;
}

/*************************************************************************/

static inline void glob_keyname_prev_level(dbats_keytree_iterator *dki)
{
    kt_node_t *kts = dki->nodes;
    int lvl = dki->lvl;

    if (kts[lvl].cursor.dbc) {
	raw_cursor_close(&kts[lvl].cursor);
    }
    dki->lvl--;
}

static int glob_keyname_next(dbats_keytree_iterator *dki, uint32_t *key_id_p,
    char *keynamebuf, uint32_t flags)
{
    int rc;
    const char *msg = NULL;
    DBT dbt_ktkey;
    DBT dbt_ktid;

// shorthands
#define lvl (dki->lvl)
#define node (&dki->nodes[lvl])

next_key:
    if (lvl == 0)
	return DB_NOTFOUND;

    if (node->cursor.dbc) {
next_glob:
	// continue with glob scan in progress
	DBT_init_out(dbt_ktkey, &node->key, sizeof(keytree_key));
	DBT_init_out(dbt_ktid, &node->id, sizeof(uint32_t));
	rc = raw_cursor_get(&node->cursor, &dbt_ktkey, &dbt_ktid, DB_NEXT);

glob_result:
	if (rc == 0 && node->key.parent == dki->nodes[lvl-1].id &&
	    strncmp(node->key.nodename, node->start, node->pfxlen) == 0)
	{
	    // found match with correct parent and nodename prefix
	    node->anlen = dbt_ktkey.size - KTKEY_SIZE(0);
	    node->key.nodename[node->anlen] = '\0';
	    if (dki->pattern) {
		char terminator = node->start[node->gnlen];
		node->start[node->gnlen] = '\0';
		int ismatch = glob_match(node->start, node->key.nodename);
		node->start[node->gnlen] = terminator;
		if (!ismatch) {
		    dbats_log(DBATS_LOG_FINEST, "Glob skip %s %x at level %d: %.*s",
			(node->id & KTID_IS_NODE) ? "node" : "leaf",
			ntohl(node->id), lvl, node->anlen+1,
			node->key.nodename);
		    goto next_glob;
		}
	    }
	    node->key.nodename[node->anlen] = '\0';
	    dbats_log(DBATS_LOG_FINEST, "Globbed %s %x at level %d: %s",
		(node->id & KTID_IS_NODE) ? "node" : "leaf",
		ntohl(node->id), lvl, node->key.nodename);
	    goto found;

	} else if (rc == 0 || rc == DB_NOTFOUND) {
	    // we've scanned past last matching node
	    dbats_log(DBATS_LOG_FINEST, "No match for parent=%x",
		ntohl(dki->nodes[lvl-1].id));
	    while (1) {
		glob_keyname_prev_level(dki);
		if (lvl == 0) return DB_NOTFOUND;
		if (node->cursor.dbc) goto next_glob;
	    }

	} else {
	    // error
	    return rc;
	}
    }

next_level:
    if (dki->pattern)
	node->gnlen = strcspn(node->start, ".");
    if (flags & DBATS_GLOB) {
	// search for a glob match for this level
	rc = raw_cursor_open(dki->dh->dbKeytree, dki->txn, &node->cursor, 0);
	if (rc != 0) { msg = "cursor"; goto logabort; }
	node->key.parent = dki->nodes[lvl-1].id;
	node->pfxlen = dki->pattern ? strcspn(node->start, "\\?[{*.") : 0;
	memcpy(node->key.nodename, node->start, node->pfxlen);
	DBT_init_in(dbt_ktkey, &node->key, KTKEY_SIZE(node->pfxlen));
	dbt_ktkey.ulen = sizeof(keytree_key);
	DBT_init_out(dbt_ktid, &node->id, sizeof(uint32_t));
	rc = raw_cursor_get(&node->cursor, &dbt_ktkey, &dbt_ktid, DB_SET_RANGE);
	goto glob_result;
    }

    // search for an exact match for this level
    if (lvl <= dki->kt_levels &&
	strncmp(node->key.nodename, node->start, node->gnlen+1) == 0)
    {
	// found in cache
	dbats_log(DBATS_LOG_FINEST, "Found cached node %x at level %d: %.*s",
	    ntohl(node->id), lvl, node->anlen + 1, node->start);
	goto found;

    } else {
	// do a db lookup
	dki->kt_levels = lvl - 1;
	keytree_key *ktkey = &node->key;
	ktkey->parent = dki->nodes[lvl-1].id;
	memcpy(ktkey->nodename, node->start, node->gnlen+1);
	uint32_t ktkey_size = KTKEY_SIZE(node->gnlen);

	// In the common case, get will succeed and we won't need to write
	// to the db, so we can get without a write lock.
	rc = raw_db_get(dki->dh->dbKeytree, dki->txn,
	    ktkey, ktkey_size, &node->id, sizeof(node->id), NULL, 0);

	if (rc == DB_NOTFOUND && (flags & DBATS_CREATE)) {
	    // We will need to write, so we retry with a write lock, in case
	    // key was created in some parallel not-yet-completed transaction.
	    rc = raw_db_get(dki->dh->dbKeytree, dki->txn,
		ktkey, ktkey_size, &node->id, sizeof(node->id), NULL, DB_RMW);
	}

	if (rc == 0) {
	    // found it
	    node->anlen = node->gnlen;
	    dbats_log(DBATS_LOG_FINEST, "Found node %x at level %d: %.*s",
		ntohl(node->id), lvl, node->anlen + 1, node->start);
	    goto found;

	} else if (rc != DB_NOTFOUND) {
	    // error
	    return rc;

	} else if (!(flags & DBATS_CREATE)) {
	    // not found, and creation not requested
	    dbats_log(DBATS_LOG_FINE, "Key not found: %s", dki->pattern);
	    while (1) {
		glob_keyname_prev_level(dki);
		if (lvl == 0) return DB_NOTFOUND;
		if (node->cursor.dbc) goto next_glob;
	    }

	} else if (dki->dh->cfg.readonly) {
	    // creation requested but not allowed
	    rc = EPERM;
	    dbats_log(DBATS_LOG_ERR, "Unable to create key %s; %s",
		dki->pattern, db_strerror(rc));
	    return rc;

	} else {
	    // create
	    if (node->gnlen > KTKEY_NODENAME_MAXLEN) {
		dbats_log(DBATS_LOG_ERR,
		    "Node name too long at level %d (%.*s) in key %s",
		    lvl, node->gnlen, node->start, dki->pattern);
		return EINVAL;
	    }
	    node->anlen = node->gnlen;

	    if (node->start[node->gnlen] == '.') {
		// create node
		if (!dki->ktseq) {
		    rc = db_sequence_create(&dki->ktseq, dki->dh->dbSequence, 0);
		    if (rc != 0) { msg = "db_sequence_create"; goto logabort; }
		    //rc = dki->ktseq->set_cachesize(dki->ktseq, 32);
		    //if (rc != 0) { msg = "seq->set_cachesize"; goto logabort; }
		    DBT_in(dbt_seq, (void*)"keytree_seq", strlen("keytree_seq"));
		    rc = dki->ktseq->open(dki->ktseq, NULL, &dbt_seq, DB_CREATE);
		    if (rc != 0) { msg = "seq->open"; goto logabort; }
		}
		db_seq_t seqnum;
		rc = dki->ktseq->get(dki->ktseq, dki->txn, 1, &seqnum,
		    DB_AUTO_COMMIT | DB_TXN_NOSYNC);
		if (rc != 0) { msg = "ktseq->get"; goto logabort; }
		node->id = htonl(seqnum) | KTID_IS_NODE;
		rc = raw_db_set(dki->dh->dbKeytree, dki->txn,
		    ktkey, ktkey_size, &node->id, sizeof(node->id), 0);
		if (rc != 0) return rc;
		dbats_log(DBATS_LOG_FINEST, "Created node #%x: %.*s",
		    ntohl(node->id), node->gnlen, node->key.nodename);
		goto found;

	    } else {
		// create leaf (metric key)
		db_recno_t recno;
		DBT_out(dbt_keyrecno, &recno, sizeof(recno)); // put() will fill
		DBT_in(dbt_keyname, (void*)dki->pattern, strlen(dki->pattern));
		rc = dki->dh->dbKeyid->put(dki->dh->dbKeyid, dki->txn,
		    &dbt_keyrecno, &dbt_keyname, DB_APPEND);
		if (rc != 0) {
		    dbats_log(DBATS_LOG_ERR, "Error creating keyid for %s: %s",
			dki->pattern, db_strerror(rc));
		    return rc;
		}
		node->id = htonl(recno - 1);
		if (recno > MAX_NUM_FRAGS * ENTRIES_PER_FRAG) {
		    dbats_log(DBATS_LOG_ERR, "Out of space for key %s", dki->pattern);
		    return ENOMEM;
		}

		rc = raw_db_set(dki->dh->dbKeytree, dki->txn, ktkey, ktkey_size,
		    &node->id, sizeof(node->id), DB_NOOVERWRITE);
		if (rc != 0) return rc;

		dbats_log(DBATS_LOG_FINEST, "Assigned key #%x: %s",
		    ntohl(node->id), dki->pattern);
		goto found;
	    }
	}
    }

found:
    if (dki->pattern) {
	if (node->start[node->gnlen] == '.') {
	    if (!(node->id & KTID_IS_NODE)) {
		// expecting node, found leaf
		if (!(flags & DBATS_GLOB)) {
		    dbats_log(DBATS_LOG_ERR, "Found leaf at level %d (%.*s) in key %s",
			lvl, node->gnlen, node->start, dki->pattern);
		    return ENOTDIR;
		}
		while (lvl > 0 && !node->cursor.dbc) {
		    glob_keyname_prev_level(dki);
		}
		goto next_key;
	    }
	    // go to next level
	    ++lvl;
	    node->start = dki->nodes[lvl-1].start + dki->nodes[lvl-1].gnlen + 1;
	    node->cursor.dbc = NULL;
	    goto next_level;
	}

    } else {
	if (node->id & KTID_IS_NODE) {
	    // We're doing a leaf walk, but this is a node.  Go to next level.
	    ++lvl;
	    node->cursor.dbc = NULL;
	    goto next_level;
	}
    }

    // generate result
    if (keynamebuf) {
	int offset = 0;
	for (int i = 1; i <= lvl; i++) {
	    offset += sprintf(keynamebuf + offset, "%s%s",
		(i > 1 ? "." : ""), dki->nodes[i].key.nodename);
	}
    }
    *key_id_p = ntohl(node->id);

    // prepare for next call
    while (lvl > 0 && !node->cursor.dbc) {
	glob_keyname_prev_level(dki);
    }

    return 0;

logabort:
    dbats_log(DBATS_LOG_ERR, "%s: %s", msg, db_strerror(rc));
    return rc;

#undef lvl
#undef node
}

static inline int dbats_glob_keyname_new(dbats_handler *dh,
    dbats_keytree_iterator **dkip, DB_TXN *txn)
{
    *dkip = emalloc(sizeof(dbats_keytree_iterator), "iterator");
    if (!*dkip) return ENOMEM;
    (*dkip)->ktseq = NULL;
    if (!((*dkip)->txn_is_mine = !txn)) {
	(*dkip)->txn = txn;
    } else {
	int rc = begin_transaction(dh, NULL, &(*dkip)->txn, "key txn");
	if (rc != 0) return rc;
    }
    return 0;
}

static int dbats_glob_keyname_reset(dbats_handler *dh,
    dbats_keytree_iterator *dki, const char *pattern)
{
    dki->dh = dh;

    dki->nodes[0].key.parent = 0;
    dki->nodes[0].id = chtonl(0) | KTID_IS_NODE;
    dki->kt_levels = 0;

    dki->lvl = 1;
    dki->nodes[1].start = dki->pattern = pattern ? strdup(pattern) : NULL;
    dki->nodes[1].cursor.dbc = NULL;

    // don't init dki->txn or dki->ktseq

    if (dki->pattern) {
	char *start = dki->pattern;
	while (1) {
	    char *end = strchr(start, '.');
	    if (end) *end = '\0';
	    if (!glob_validate(start))
		return EINVAL;
	    if (!end) break;
	    *end = '.';
	    start = end + 1;
	}
    }

    return 0;
}

int dbats_glob_keyname_start(dbats_handler *dh, dbats_snapshot *ds,
    dbats_keytree_iterator **dkip, const char *pattern)
{
    int rc = dbats_glob_keyname_new(dh, dkip, ds ? ds->txn : dh->txn);
    if (rc != 0) return rc;
    return dbats_glob_keyname_reset(dh, *dkip, pattern);
}

int dbats_glob_keyname_next(dbats_keytree_iterator *dki, uint32_t *key_id_p,
    char *namebuf)
{
    return glob_keyname_next(dki, key_id_p, namebuf, DBATS_GLOB);
}

static void dbats_glob_keyname_clear(dbats_keytree_iterator *dki)
{
    while (dki->lvl)
	glob_keyname_prev_level(dki);
    if (dki->pattern) free(dki->pattern);
    dki->pattern = NULL;
    // don't close dki->ktseq
}

int dbats_glob_keyname_end(dbats_keytree_iterator *dki)
{
    int rc = 0;
    dbats_glob_keyname_clear(dki);
    if (dki->ktseq)
	dki->ktseq->close(dki->ktseq, 0);
    if (dki->txn_is_mine)
	rc = commit_transaction(dki->dh, dki->txn); // key txn
    free(dki);
    return rc;
}

int dbats_bulk_get_key_id(dbats_handler *dh, dbats_snapshot *ds,
    uint32_t n_keys, const char * const *keys, uint32_t *key_ids, uint32_t flags)
{
    int rc = 0;

    dbats_keytree_iterator *dki;
    rc = dbats_glob_keyname_new(dh, &dki, ds ? ds->txn : dh->txn);
    if (rc != 0) return rc;

    for (int i = 0; i < n_keys; i++) {
	rc = dbats_glob_keyname_reset(dh, dki, keys[i]);
	if (rc != 0) break;
	rc = glob_keyname_next(dki, &key_ids[i], NULL, flags & ~DBATS_GLOB);
	if (rc != 0) break;
	if (key_ids[i] & DBATS_KEY_IS_PREFIX) {
	    dbats_log(DBATS_LOG_ERR, "Key %s is only a prefix", keys[i]);
	    rc = EISDIR;
	    break;
	}
	dbats_log(DBATS_LOG_FINEST, "Found key #%u: %s", key_ids[i],
	    keys[i]);
	dbats_glob_keyname_clear(dki);
    }

    dbats_glob_keyname_end(dki);
    return rc;
}

int dbats_get_key_id(dbats_handler *dh, dbats_snapshot *ds, const char *key,
    uint32_t *key_id_p, uint32_t flags)
{
    return dbats_bulk_get_key_id(dh, ds, 1, &key, key_id_p, flags);
}

int dbats_get_key_name(dbats_handler *dh, dbats_snapshot *ds,
    uint32_t key_id, char *namebuf)
{
    int rc;
    db_recno_t recno = key_id + 1;
    uint32_t value_len;

    rc = raw_db_get(dh->dbKeyid, ds->txn, &recno, sizeof(recno),
	namebuf, DBATS_KEYLEN - 1, &value_len, DB_READ_COMMITTED);
    if (rc != 0) return rc;
    namebuf[value_len] = '\0';
    return 0;
}

/*************************************************************************/

static int instantiate_frag_func(dbats_snapshot *ds, int bid, uint32_t frag_id)
{
    int rc;
    dbats_tslice *tslice = ds->tslice[bid];

    if (!ds->preload || ds->readonly) {
	// Try to load the fragment.
	rc = load_frag(ds, tslice->time, bid, frag_id);
	if (rc != 0) // error
	    return rc;
	if (tslice->frag[frag_id]) { // found it
	    if (bid == 0)
		rc = instantiate_isset_frags(ds, frag_id);
	    return rc;
	}
	if (ds->readonly) // didn't find it and can't create it
	    return DB_NOTFOUND; // fragment not found
    }

    // Allocate a new fragment.
    dbats_log(DBATS_LOG_VFINE, "grow tslice for time %u, bid %d, frag_id %u",
	tslice->time, bid, frag_id);

    size_t len = fragsize(ds->dh);
    if (!(tslice->frag[frag_id] = ecalloc(1, len, "tslice->frag[frag_id]"))) {
	return errno ? errno : ENOMEM;
    }

    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;
    if (bid == 0) {
	rc = instantiate_isset_frags(ds, frag_id);
	if (rc != 0) return rc;
    }

    dbats_log(DBATS_LOG_VFINE, "Grew tslice to %u elements",
	tslice->num_frags * ENTRIES_PER_FRAG);

    return 0;
}

// inline version handles the common case without a function call
static inline int instantiate_frag(dbats_snapshot *ds, int bid, uint32_t frag_id)
{
    return ds->tslice[bid]->frag[frag_id] ?
	0 : // We already have the fragment.
	instantiate_frag_func(ds, bid, frag_id);
}

/*************************************************************************/

int dbats_set(dbats_snapshot *ds, uint32_t key_id, const dbats_value *valuep)
{
    dbats_log(DBATS_LOG_VFINE, "dbats_set %u #%u = %" PRIu64,
	ds->tslice[0]->time, key_id, valuep[0].u64); // XXX
    int rc;
    dbats_handler *dh = ds->dh;

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

    if (!dh->is_open || ds->readonly) {
	dbats_log(DBATS_LOG_ERR, "is_open=%d, readonly=%d",
	    dh->is_open, ds->readonly);
	return EPERM;
    }

    if (!ds->tslice || !ds->tslice[0] || !ds->tslice[0]->time) {
	dbats_log(DBATS_LOG_ERR, "dbats_set() without dbats_select_snap()");
	return -1;
    }

    if ((rc = instantiate_frag(ds, 0, frag_id)) != 0)
	return rc;

    uint8_t was_set = vec_test(ds->tslice[0]->is_set[frag_id], offset);
    if (was_set & !dh->cfg.updatable) {
	dbats_log(DBATS_LOG_ERR, "value is already set: t=%" PRIu32 " keyid=%d",
	    ds->tslice[0]->time, key_id);
	return DB_KEYEXIST;
    }
    dbats_value *oldvaluep = valueptr(ds, 0, frag_id, offset);

    // For each bid, aggregate *valuep into tslice[bid].
    for (int bid = 1; bid < ds->num_bundles; bid++) {

	if (dh->bundle[bid].keep > 0) {
	    // Skip if the time is outside of bundle's keep limit
	    uint32_t keep_period = (dh->bundle[bid].keep-1) * dh->bundle[bid].period;
	    if (keep_period < ds->end_time &&
		ds->tslice[bid]->time < ds->end_time - keep_period)
		    continue;
	}

	if ((rc = instantiate_frag(ds, bid, frag_id)) != 0)
	    return rc;

	uint8_t changed = 0;
	uint8_t failed = 0;
	dbats_value *aggval = valueptr(ds, bid, frag_id, offset);
	uint32_t aggstart = ds->tslice[bid]->time;
	uint32_t aggend = min(ds->active_last_data,
	    aggstart + dh->bundle[bid].period - dh->bundle[0].period);

	// Count the number of steps contributing to the aggregate.
	int n = 0;
	{
	    int ti = (aggstart - ds->active_start) / dh->bundle[0].period;
	    uint32_t t = aggstart;
	    while (t <= aggend) {
		if (t == ds->tslice[0]->time ||
		    vec_test(ds->is_set[ti][frag_id], offset))
			n++;
		ti++;
		t += dh->bundle[0].period;
	    }
	}

	dbats_log(DBATS_LOG_FINEST, "agg %d: aggval=%" PRIu64 " n=%d",
	    bid, aggval[0].u64, n); // XXX

	for (int i = 0; i < dh->cfg.values_per_entry; i++) {
	    if (was_set && valuep[i].u64 == oldvaluep[i].u64) {
		continue; // value did not change; no need to change agg value
	    }
	    switch (dh->bundle[bid].func) {
	    case DBATS_AGG_MIN:
		if (n == 1 || valuep[i].u64 <= aggval[i].u64) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else if (was_set && oldvaluep[i].u64 == aggval[i].u64) {
		    // XXX TODO: Find the min value among all the steps.
		    failed = ENOSYS; // XXX
		}
		break;
	    case DBATS_AGG_MAX:
		if (n == 1 || valuep[i].u64 >= aggval[i].u64) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else if (was_set && oldvaluep[i].u64 == aggval[i].u64) {
		    // XXX TODO: Find the max value among all the steps.
		    failed = ENOSYS; // XXX
		}
		break;
	    case DBATS_AGG_AVG:
		{
		    if (n == 1) {
			aggval[i].d = valuep[i].u64;
			changed = 1;
		    } else {
			double old_daggval = aggval[i].d;
			if (was_set)
			    aggval[i].d -= (oldvaluep[i].u64 - aggval[i].d) / n;
			aggval[i].d += (valuep[i].u64 - aggval[i].d) / n;
			changed = (aggval[i].d != old_daggval);
		    }
		}
		break;
	    case DBATS_AGG_LAST:
		if (ds->tslice[0]->time >= ds->active_last_data) {
		    // common case: value is latest ever seen
		    aggval[i] = valuep[i];
		    changed = 1;
		} else {
		    // find time of last sample for this key within agg period
		    uint32_t t = aggend;
		    int ti = (t - ds->active_start) / dh->bundle[0].period;
		    while (t >= aggstart) {
			if (t == ds->tslice[0]->time) {
			    // the new value is the last
			    aggval[i] = valuep[i];
			    changed = 1;
			    break;
			}
			if (vec_test(ds->is_set[ti][frag_id], offset))
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
		} else if (was_set || valuep[i].u64 != 0) {
		    if (was_set)
			aggval[i].u64 -= oldvaluep[i].u64;
		    aggval[i].u64 += valuep[i].u64;
		    changed = 1;
		}
		break;
	    }
	}

	if (changed || failed) {
	    int level = failed ? DBATS_LOG_ERR : DBATS_LOG_VFINE;
	    if (level <= dbats_log_level) { 
		char aggbuf[64];
		if (dh->bundle[bid].func == DBATS_AGG_AVG)
		    sprintf(aggbuf, "%f", aggval[0].d);
		else
		    sprintf(aggbuf, "%" PRIu64, aggval[0].u64);
		dbats_log(level, "%s set value bid=%d frag_id=%u "
		    "offset=%" PRIu32 " value_len=%u aggval=%s",
		    failed ? "Failed to" : "Successfully",
		    bid, frag_id, offset, dh->cfg.entry_size, aggbuf);
	    }
	}
	if (changed) {
	    // XXX if (n >= xff * steps)
	    vec_set(ds->tslice[bid]->is_set[frag_id], offset);
	    ds->tslice[bid]->frag_changed[frag_id] = 1;
	} else if (failed) {
	    return failed;
	}
    }

    // Set value at bid 0 (after aggregations because aggregations need
    // both old and new values)
    memcpy(oldvaluep, valuep, dh->cfg.entry_size);
    vec_set(ds->tslice[0]->is_set[frag_id], offset);

    if (!ds->changed) {
	// Once we've started writing, increase priority over other processes
	// that haven't yet done any writing.
	set_priority(ds, (ds->tslice[0]->time + INT32_MAX) / dh->bundle[0].period);
	ds->changed = 1;
    }
    ds->tslice[0]->frag_changed[frag_id] = 1;

    dbats_log(DBATS_LOG_VFINE, "Succesfully set value "
	"bid=%d frag_id=%u offset=%" PRIu32 " value_len=%u value=%" PRIu64,
	0, frag_id, offset, dh->cfg.entry_size, valuep[0].u64);

    return 0;
}

int dbats_set_by_key(dbats_snapshot *ds, const char *key,
    const dbats_value *valuep, int flags)
{
    int rc;
    uint32_t key_id;

    if ((rc = dbats_get_key_id(ds->dh, ds, key, &key_id, flags)) != 0)
	return rc;
    return dbats_set(ds, key_id, valuep);
}

/*************************************************************************/

static inline int alloc_valbuf(dbats_snapshot *ds)
{
    if (!ds->valbuf) {
	ds->valbuf = emalloc(ds->dh->cfg.entry_size * ds->dh->cfg.values_per_entry,
	    "valbuf");
	if (!ds->valbuf)
	    return errno ? errno : ENOMEM;
    }
    return 0;
}

int dbats_get(dbats_snapshot *ds, uint32_t key_id,
    const dbats_value **valuepp, int bid)
{
    int rc;
    dbats_tslice *tslice = ds->tslice[bid];
    dbats_handler *dh = ds->dh;

    if (!dh->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if (!ds->tslice || !ds->tslice[0] || !ds->tslice[0]->time) {
	dbats_log(DBATS_LOG_ERR, "dbats_get() without dbats_select_snap()");
	return -1;
    }

    uint32_t frag_id = keyfrag(key_id);
    uint32_t offset = keyoff(key_id);

    if ((rc = instantiate_frag(ds, bid, frag_id)) == 0) {
	if (!vec_test(tslice->is_set[frag_id], offset)) {
	    char keyname[DBATS_KEYLEN] = "";
	    dbats_get_key_name(dh, ds, key_id, keyname);
	    dbats_log(DBATS_LOG_WARN, "Value unset (v): %u %d %s",
		tslice->time, bid, keyname);
	    *valuepp = NULL;
	    return DB_NOTFOUND;
	}
	*valuepp = valueptr(ds, bid, frag_id, offset);
	if (dh->swapped || dh->bundle[bid].func == DBATS_AGG_AVG) {
	    if ((rc = alloc_valbuf(ds)) != 0) return rc;
	}
	if (dh->swapped) {
	    for (int i = 0; i < dh->cfg.values_per_entry; i++)
		ds->valbuf[i].u64 = cswap64((*valuepp)[i].u64);
	    *valuepp = ds->valbuf;
	} else if (dh->bundle[bid].func == DBATS_AGG_AVG) {
	    for (int i = 0; i < dh->cfg.values_per_entry; i++)
		ds->valbuf[i] = (*valuepp)[i];
	    *valuepp = ds->valbuf;
	}
	dbats_log(DBATS_LOG_VFINE, "Succesfully read value off=%" PRIu32 " len=%u",
	    offset, dh->cfg.entry_size);
	return 0;

    } else if (rc == DB_NOTFOUND) {
	char keyname[DBATS_KEYLEN] = "";
	dbats_get_key_name(dh, ds, key_id, keyname);
	dbats_log(DBATS_LOG_WARN, "Value unset (f): %u %d %s",
	    tslice->time, bid, keyname);
	*valuepp = NULL;
	return rc;

    } else {
	return rc;
    }
}

int dbats_get_by_key(dbats_snapshot *ds, const char *key,
    const dbats_value **valuepp, int bid)
{
    int rc;
    uint32_t key_id;

    if ((rc = dbats_get_key_id(ds->dh, ds, key, &key_id, 0)) != 0) {
	return rc;
    }
    return dbats_get(ds, key_id, valuepp, bid);
}

/*************************************************************************/

int dbats_walk_keyid_start(dbats_handler *dh, dbats_snapshot *ds,
    dbats_keyid_iterator **dkip)
{
    *dkip = emalloc(sizeof(dbats_keyid_iterator), "iterator");
    if (!*dkip) return ENOMEM;
    DB_TXN *txn = ds ? ds->txn : dh->txn;
    int rc = raw_cursor_open(dh->dbKeyid, txn, &(*dkip)->cursor, 0);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "Error in dbats_walk_keyid_start: %s",
	    db_strerror(rc));
	return -1;
    }
    return 0;
}

int dbats_walk_keyid_next(dbats_keyid_iterator *dki, uint32_t *key_id_p,
    char *namebuf)
{
    int rc;
    db_recno_t recno;

    DBT_out(dbt_keyrecno, &recno, sizeof(recno));
    DBT dbt_keyname;
    if (namebuf) {
	DBT_init_out(dbt_keyname, namebuf, DBATS_KEYLEN-1);
    } else {
	DBT_init_null(dbt_keyname);
    }

    rc = raw_cursor_get(&dki->cursor, &dbt_keyrecno, &dbt_keyname,
	DB_NEXT | DB_READ_COMMITTED);
    if (rc != 0) {
	if (rc != DB_NOTFOUND)
	    dbats_log(DBATS_LOG_ERR, "Error in dbats_walk_keyid_next: %s",
		db_strerror(rc));
	return -1;
    }
    *key_id_p = recno - 1;
    if (namebuf)
	namebuf[dbt_keyname.size] = '\0';
    return 0;
}

int dbats_walk_keyid_end(dbats_keyid_iterator *dki)
{
    int rc = raw_cursor_close(&dki->cursor);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "Error in dbats_walk_keyid_end: %s",
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
    
    if ((rc = dh->dbConfig->stat_print(dh->dbConfig, DB_FAST_STAT)) != 0)
	dbats_log(DBATS_LOG_ERR, "dumping Config stats: %s", db_strerror(rc));
    if ((rc = dh->dbBundle->stat_print(dh->dbBundle, DB_FAST_STAT)) != 0)
	dbats_log(DBATS_LOG_ERR, "dumping Bundle stats: %s", db_strerror(rc));
    if ((rc = dh->dbKeytree->stat_print(dh->dbKeytree, DB_FAST_STAT)) != 0)
	dbats_log(DBATS_LOG_ERR, "dumping Keytree stats: %s", db_strerror(rc));
    if ((rc = dh->dbKeyid->stat_print(dh->dbKeyid, DB_FAST_STAT)) != 0)
	dbats_log(DBATS_LOG_ERR, "dumping Keyid stats: %s", db_strerror(rc));
    if ((rc = dh->dbValues->stat_print(dh->dbValues, DB_FAST_STAT)) != 0)
	dbats_log(DBATS_LOG_ERR, "dumping Values stats: %s", db_strerror(rc));
    if ((rc = dh->dbIsSet->stat_print(dh->dbIsSet, DB_FAST_STAT)) != 0)
	dbats_log(DBATS_LOG_ERR, "dumping IsSet stats: %s", db_strerror(rc));
    if ((rc = dh->dbSequence->stat_print(dh->dbSequence, DB_FAST_STAT)) != 0)
	dbats_log(DBATS_LOG_ERR, "dumping Sequence stats: %s", db_strerror(rc));
}

