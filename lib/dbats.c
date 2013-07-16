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

#include "dbats.h"
#include "quicklz.h"

// Limits
#define ENTRIES_PER_FRAG    10000 // number of entries in a fragment
#define MAX_NUM_FRAGS       16384 // max number of fragments in a tslice
#define KEYINFO_PER_BLK     10000 // number of key_info in a block
#define MAX_NUM_KEYINFOBLK  16384 // max number of key_info blocks
#define MAX_NUM_AGGS           16 // max number of aggregations


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
    uint8_t is_set[vec_size(ENTRIES_PER_FRAG)]; // which columns have values
    uint8_t data[]; // values (C99 flexible array member)
} dbats_frag;

// Logical row or "time slice", containing all the entries for a given time
// (broken into fragments).
typedef struct dbats_tslice {
    uint32_t time;                        // start time (unix seconds)
    uint32_t num_frags;                   // number of fragments
    uint8_t preload;                      // load frags when tslice is selected
    dbats_frag *frag[MAX_NUM_FRAGS];
    uint8_t frag_changed[MAX_NUM_FRAGS];
} dbats_tslice;

#define fragsize(handler) (sizeof(dbats_frag) + ENTRIES_PER_FRAG * (handler)->cfg.entry_size)

typedef struct {
    uint32_t time;
    int agg_id;
    uint32_t frag_id;
} fragkey_t;

typedef
struct dbats_key_info {
    const char *key;
    uint32_t key_id;               // id of key/column
    uint32_t frag_id;              // fragment within timeslice
    uint32_t offset;               // index within fragment
    uint32_t n_timeranges;         // number of timeranges
    dbats_timerange_t *timeranges; // When did this key actually have a value?
} dbats_key_info_t;

// key_info as stored in db
typedef struct {
    uint32_t key_id;
    dbats_timerange_t timeranges[]; // When did this key have a value? (C99 flexible array member)
} raw_key_info_t;

struct dbats_handler {
    dbats_config cfg;
    uint8_t is_open;
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
};


/************************************************************************
 * utilities
 ************************************************************************/

#if 1
static void *emalloc(size_t size, const char *msg)
{
    void *ptr = malloc(size);
    if (!ptr)
	dbats_log(LOG_ERROR, "Can't allocate %zu bytes for %s", size, msg);
    return ptr;
}
#else
# define emalloc(size, msg) xmalloc(size)
#endif

/************************************************************************
 * DB wrappers
 ************************************************************************/

static int raw_db_open(dbats_handler *handler, DB **dbp, const char *envpath,
    const char *dbname, uint32_t flags, int mode)
{
    int ret;

    if ((ret = db_create(dbp, handler->dbenv, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error creating DB handler %s: %s",
	    dbname, db_strerror(ret));
	return -1;
    }

    uint32_t dbflags = 0;
    if (flags & DBATS_CREATE)
	dbflags |= DB_CREATE;
    if (flags & DBATS_READONLY)
	dbflags |= DB_RDONLY;

    ret = (*dbp)->open(*dbp, NULL, dbname, NULL, DB_BTREE, dbflags, mode);
    if (ret != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s/%s (mode=%o): %s",
	    envpath, dbname, mode, db_strerror(ret));
	return -1;
    }
    return 0;
}

static void raw_db_set(const dbats_handler *handler, DB *db,
    const void *key, uint32_t key_len,
    const void *value, uint32_t value_len)
{
    DBT key_data, data;
    int ret;

    if (db == handler->dbData) {
	fragkey_t *fragkey = (fragkey_t*)key;
	dbats_log(LOG_INFO, "raw_db_set Data: t=%u agg=%d frag=%u, compress=%d, len=%u",
	    fragkey->time, fragkey->agg_id, fragkey->frag_id, *(uint8_t*)value, value_len);
    }

    if (handler->cfg.readonly) {
	const char *dbname;
	db->get_dbname(db, NULL, &dbname);
	dbats_log(LOG_WARNING, "Unable to set value in database \"%s\" "
	    "(read-only mode)", dbname);
	return;
    }

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));
    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    key_data.flags = DB_DBT_USERMEM;
    data.data = (void*)value; // assumption: DB won't write to *data.data
    data.size = value_len;
    data.flags = DB_DBT_USERMEM;

    if ((ret = db->put(db, NULL, &key_data, &data, 0)) != 0)
	dbats_log(LOG_WARNING, "Error in raw_db_set: %s", db_strerror(ret));
}

#define QLZ_OVERHEAD 400

static int raw_db_get(dbats_handler *handler, DB* db,
    const void *key, uint32_t key_len, void **value, size_t *value_len)
{
    DBT key_data, data;
    int rc;

    if (handler->db_get_buf_len < fragsize(handler) + QLZ_OVERHEAD) {
	if (handler->db_get_buf) free(handler->db_get_buf);
	handler->db_get_buf_len = fragsize(handler) + QLZ_OVERHEAD;
	handler->db_get_buf = emalloc(handler->db_get_buf_len, "get buffer");
	if (!handler->db_get_buf) return -2;
    }

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));

    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    key_data.flags = DB_DBT_USERMEM;
    data.data = handler->db_get_buf;
    data.ulen = handler->db_get_buf_len;
    data.flags = DB_DBT_USERMEM;
    if ((rc = db->get(db, NULL, &key_data, &data, 0)) == 0) {
	*value = data.data;
	*value_len = data.size;
	return 0;
    }
    if (rc != DB_NOTFOUND)
	dbats_log(LOG_INFO, "raw_db_get failed: %s", db_strerror(rc));
    if (rc == DB_BUFFER_SMALL) {
	dbats_log(LOG_WARNING, "raw_db_get: had %" PRIu32 ", needed %" PRIu32,
	    data.ulen, data.size);
    }
    return -1;
}

/*
static void raw_db_delete(const dbats_handler *handler, DB *db,
    const void *key, uint32_t key_len)
{
    DBT key_data;

    if (handler->cfg.readonly) {
	dbats_log(LOG_WARNING, "Unable to delete value (read-only mode)");
	return;
    }

    memset(&key_data, 0, sizeof(key_data));
    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    key_data.flags = DB_DBT_USERMEM;

    if (db->del(db, NULL, &key_data, 0) != 0)
	dbats_log(LOG_WARNING, "Error while deleting key");
}
*/

/*
static int raw_db_key_exists(const dbats_handler *handler, DB *db,
    const void *key, size_t key_len)
{
    void *value;
    size_t value_len;

    return (raw_db_get(handler, db, key, key_len, &value, &value_len) == 0);
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
	size_t size = sizeof(dbats_key_info_t) * KEYINFO_PER_BLK;
	handler->key_info_block[block] = emalloc(size, "key_info_block");
	if (!handler->key_info_block[block])
	    return NULL;
	memset(handler->key_info_block[block], 0, size);
    }
    dbats_key_info_t *dkip = &handler->key_info_block[block][offset];
    dkip->key = keycopy;
    dkip->key_id = key_id;
    dkip->frag_id = key_id / ENTRIES_PER_FRAG;
    dkip->offset = key_id % ENTRIES_PER_FRAG;
    return dkip;
}

dbats_handler *dbats_open(const char *path,
    uint16_t values_per_entry,
    uint32_t period,
    uint32_t flags)
{
    int ret;
    int dbmode = 00666;
    uint32_t dbflags = DB_INIT_MPOOL;
    int is_new = 0;

    dbats_handler *handler = emalloc(sizeof(dbats_handler), "handler");
    if (!handler) return NULL;
    memset(handler, 0, sizeof(dbats_handler));
    handler->cfg.readonly = !!(flags & DBATS_READONLY);
    handler->cfg.compress = !(flags & DBATS_UNCOMPRESSED);
    handler->cfg.values_per_entry = 1;

    if ((ret = db_env_create(&handler->dbenv, 0)) != 0) {
	dbats_log(LOG_ERROR, "Error creating DB env: %s",
	    db_strerror(ret));
	return NULL;
    }

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

    if ((ret = handler->dbenv->open(handler->dbenv, path, dbflags, dbmode)) != 0) {
	dbats_log(LOG_ERROR, "Error opening DB %s: %s", path, db_strerror(ret));
	return NULL;
    }

    if (raw_db_open(handler, &handler->dbMeta, path, "meta", flags, dbmode) != 0) return NULL;
    if (raw_db_open(handler, &handler->dbKeys, path, "keys", flags, dbmode) != 0) return NULL;
    if (raw_db_open(handler, &handler->dbData, path, "data", flags, dbmode) != 0) return NULL;

    handler->cfg.entry_size = sizeof(dbats_value); // for db_get_buf_len

#define initcfg(type, field, defaultval) \
    do { \
	if (is_new) { \
	    handler->cfg.field = (defaultval); \
	    raw_db_set(handler, handler->dbMeta, #field, strlen(#field), \
		&handler->cfg.field, sizeof(type)); \
	} else { \
	    void *value; \
	    size_t value_len; \
	    if (raw_db_get(handler, handler->dbMeta, #field, strlen(#field), &value, &value_len) == 0) { \
		handler->cfg.field = *((type*)value); \
	    } else { \
		dbats_log(LOG_ERROR, "%s: missing config value %s", path, #field); \
		return NULL; \
	    } \
	} \
	dbats_log(LOG_INFO, "cfg: %s = %u", #field, handler->cfg.field); \
    } while (0)

    initcfg(uint32_t, num_keys,          0);
    initcfg(uint32_t, period,            period);
    initcfg(uint16_t, values_per_entry,  values_per_entry);
    initcfg(uint16_t, num_aggs,          1);

    if (handler->cfg.num_aggs > MAX_NUM_AGGS) {
	dbats_log(LOG_ERROR, "num_aggs %d > %d", handler->cfg.num_aggs,
	    MAX_NUM_AGGS);
	return NULL;
    }

    handler->agg = emalloc(MAX_NUM_AGGS * sizeof(dbats_agg), "handler->agg");
    if (!handler->agg) return NULL;
    handler->tslice = emalloc(MAX_NUM_AGGS * sizeof(dbats_tslice*), "handler->tslice");
    if (!handler->tslice) return NULL;
    handler->key_info_block = emalloc(MAX_NUM_KEYINFOBLK * sizeof(dbats_key_info_t*), "handler->key_info_block");
    if (!handler->tslice) return NULL;

    handler->cfg.entry_size = handler->cfg.values_per_entry * sizeof(dbats_value);

    // load metadata for aggregates
    if (is_new) {
	handler->agg[0].func = DBATS_AGG_NONE;
	handler->agg[0].steps = 1;
	handler->agg[0].period = handler->cfg.period;
    } else {
	void *value;
	size_t value_len;
	char keybuf[32];
	for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	    sprintf(keybuf, "agg%d", agg_id);
	    if (raw_db_get(handler, handler->dbMeta, keybuf, strlen(keybuf), &value, &value_len) != 0) {
		dbats_log(LOG_ERROR, "missing aggregation config %d", agg_id);
		return NULL;
	    } else if (value_len != sizeof(dbats_agg)) {
		dbats_log(LOG_ERROR, "corrupt aggregation config: size %zu != %zu",
		    value_len, sizeof(dbats_agg));
		return NULL;
	    }
	    memcpy(&handler->agg[agg_id], value, value_len);
	}
    }

    // allocate tslice for each aggregate
    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	if (!(handler->tslice[agg_id] = emalloc(sizeof(dbats_tslice), "tslice")))
	    return NULL;
	memset(handler->tslice[agg_id], 0, sizeof(dbats_tslice));
    }

    // load key_info for every key
    int rc;
    DBC *cursor;
    if ((rc = handler->dbKeys->cursor(handler->dbKeys, NULL, &cursor, 0)) != 0) {
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

	size_t tr_size = dbdata.size - sizeof(raw_key_info_t);
	dkip->n_timeranges = tr_size / sizeof(dbats_timerange_t);
	if (dkip->n_timeranges) {
	    dkip->timeranges = emalloc(tr_size, "timeranges");
	    if (!dkip->timeranges) return NULL;
	    memcpy(dkip->timeranges, rki->timeranges, tr_size);
	}
    }

    if ((rc = cursor->close(cursor)) != 0) {
	dbats_log(LOG_ERROR, "Error in cursor->close: %s", db_strerror(rc));
	return NULL;
    }

    handler->is_open = 1;
    return handler;
}

int dbats_aggregate(dbats_handler *handler, int func, int steps)
{
    if (handler->tslice[0]->time > 0) {
	// XXX fix this?
	dbats_log(LOG_ERROR,
	    "Adding a new aggregation to existing data is not yet supported.");
	return -1;
    }

    if (handler->cfg.num_aggs >= MAX_NUM_AGGS) {
	dbats_log(LOG_ERROR, "Too many aggregations (%d)", MAX_NUM_AGGS);
	return -1;
    }

    int agg_id = handler->cfg.num_aggs++;

    if (!(handler->tslice[agg_id] = emalloc(sizeof(dbats_tslice), "tslice")))
	return -1;
    memset(handler->tslice[agg_id], 0, sizeof(dbats_tslice));

    handler->agg[agg_id].func = func;
    handler->agg[agg_id].steps = steps;
    handler->agg[agg_id].period = handler->agg[0].period * steps;

    char keybuf[32];
    sprintf(keybuf, "agg%d", agg_id);
    raw_db_set(handler, handler->dbMeta, keybuf, strlen(keybuf),
	&handler->agg[agg_id], sizeof(dbats_agg));
    raw_db_set(handler, handler->dbMeta, "num_aggs", strlen("num_aggs"),
	&handler->cfg.num_aggs, sizeof(handler->cfg.num_aggs));

    return 0;
}

/*************************************************************************/

// Writes tslice to db (unless db is readonly).
// Also cleans up in-memory tslice, even if db is readonly.
static void dbats_flush_tslice(dbats_handler *handler, int agg_id)
{
    dbats_log(LOG_INFO, "Flush t=%u agg_id=%d", handler->tslice[agg_id]->time, agg_id);
    size_t frag_size = fragsize(handler);
    fragkey_t dbkey;
    char *buf = NULL;

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
	} else if (handler->cfg.compress) {
	    if (!handler->state_compress) {
		handler->state_compress = emalloc(sizeof(qlz_state_compress), "qlz_state_compress");
		if (!handler->state_compress) return;
	    }
	    if (!buf) {
		buf = (char*)emalloc(frag_size + QLZ_OVERHEAD + 1, "compression buffer");
		if (!buf) return;
	    }
	    buf[0] = 1; // compression flag
	    handler->tslice[agg_id]->frag[f]->compressed = 1;
	    size_t compressed_len = qlz_compress(handler->tslice[agg_id]->frag[f],
		buf+1, frag_size, handler->state_compress);
	    dbats_log(LOG_INFO, "Frag %d compression: %zu -> %zu (%.1f %%)",
		f, frag_size, compressed_len, compressed_len*100.0/frag_size);
	    raw_db_set(handler, handler->dbData, &dbkey, sizeof(dbkey),
		buf, compressed_len + 1);
	} else {
	    if (!buf)
		buf = (char*)emalloc(frag_size + 1, "write buffer");
	    handler->tslice[agg_id]->frag[f]->compressed = 0;
	    dbats_log(LOG_INFO, "Frag %d write: %zu", f, frag_size);
	    raw_db_set(handler, handler->dbData, &dbkey, sizeof(dbkey),
		handler->tslice[agg_id]->frag[f], frag_size);
	}

	handler->tslice[agg_id]->frag_changed[f] = 0;
	free(handler->tslice[agg_id]->frag[f]);
	handler->tslice[agg_id]->frag[f] = 0;
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
	    raw_db_set(handler, handler->dbMeta, keybuf, strlen(keybuf),
		&handler->agg[agg_id], sizeof(dbats_agg));
	}
    }

    memset(handler->tslice[agg_id], 0, sizeof(dbats_tslice));
    if (buf) free(buf);
}

/*************************************************************************/

static int set_key_info(const dbats_handler *handler, dbats_key_info_t *dkip)
{
    size_t tr_size = dkip->n_timeranges * sizeof(dbats_timerange_t);
    size_t rki_size = sizeof(raw_key_info_t) + tr_size;
    raw_key_info_t *rki = emalloc(rki_size, dkip->key);
    if (!rki) return -1;
    rki->key_id = dkip->key_id;
    memcpy(rki->timeranges, dkip->timeranges, tr_size);
    raw_db_set(handler, handler->dbKeys, dkip->key, strlen(dkip->key), rki, rki_size);
    free(rki);
    dbats_log(LOG_INFO, "Write key #%u: %s, %d timeranges",
	dkip->key_id, dkip->key, dkip->n_timeranges);
    /*
    int totalsteps = 0; // XXX
    for (int i = 0; i < dkip->n_timeranges; i++) { // XXX
	int steps = (dkip->timeranges[i].end + handler->agg[0].period - dkip->timeranges[i].start) /
	    handler->agg[0].period;
	dbats_log(LOG_INFO, "     %3d: %10u %10u (%3d)",
	    i, dkip->timeranges[i].start, dkip->timeranges[i].end, steps);
	totalsteps += steps;
    }
    dbats_log(LOG_INFO, "                              %6d", totalsteps); // XXX
    */
    return 0;
}

// Insert a new timerange into dkip->timeranges at position i, and initialize it
static dbats_timerange_t *timerange_insert(dbats_key_info_t *dkip, int i,
    uint32_t start, uint32_t end)
{
    int n = ++dkip->n_timeranges;
    dbats_timerange_t *tr = dkip->timeranges;
    dbats_timerange_t *newtr = emalloc(n * sizeof(dbats_timerange_t), "timeranges");
    if (!newtr) return NULL;
    if (tr) {
	memcpy(&newtr[0], &tr[0], i * sizeof(dbats_timerange_t));
	memcpy(&newtr[i+1], &tr[i], (n-1-i) * sizeof(dbats_timerange_t));
	free(tr);
    }
    newtr[i].start = start;
    newtr[i].end = end;
    return dkip->timeranges = newtr;
}

// Update the timeranges associated with each key.
static int update_key_info(dbats_handler *handler, int next_is_far)
{
    dbats_agg *agg0 = &handler->agg[0];
    dbats_tslice *tslice = handler->tslice[0];
    if (handler->cfg.readonly || tslice->time == 0) return 0;
    int is_last = tslice->time >= agg0->times.end;
    dbats_log(LOG_INFO,
	"update_key_info: t=%u, last_flush=%u, is_last=%d, next_is_far=%d",
	tslice->time, agg0->times.end, is_last, next_is_far); // XXX

#define debugUKI(first, label, i) \
    dbats_log(LOG_INFO, \
	"%s key #%u, t=%u, [%u..%u] %s %d/%d", \
	(first ? "update_key_info:" : "             -->"), \
	key_id, tslice->time, tr[i].start, tr[i].end, label, i, dkip->n_timeranges)

    for (uint32_t key_id = 0; key_id < handler->cfg.num_keys; key_id++) {
	dbats_key_info_t *dkip = find_key(handler, key_id);
	dbats_timerange_t *tr = dkip->timeranges; // shorthand

	int i = dkip->n_timeranges - 1;
	int tr_changed = 0; // timerange changed?

	// Should timerange change due to next_is_far?

	if (next_is_far && i >= 0 && tr[i].end == 0) {
	    // next time > active time + 1 step (uncommon).
	    // Terminate the current (unterminated) timerange.
	    tr_changed++;
	    debugUKI(1, "next_is_far", i); // XXX
	    tr[i].end = agg0->times.end;
	    debugUKI(0, "next_is_far", i); // XXX
	}

	// Should timerange change because value's set/unset state changed?

	if (!tslice->frag[dkip->frag_id]) {
	    // Fragment wasn't loaded, so value's state could not have changed.
	    if (tslice->time > agg0->times.end) {
		// This is the first visit to the current timeslice, so the
		// value in this timeslice could never have been set before.
		if (i >= 0 && tr[i].end == 0) {
		    // Key has a current timerange; we must terminate it.
		    tr_changed++;
		    debugUKI(1, "terminate", i);
		    tr[i].end = agg0->times.end;
		    debugUKI(0, "terminate", i);
		}
	    }

	} else if (vec_test(tslice->frag[dkip->frag_id]->is_set, dkip->offset)) {
	    // Value is set.
	    if (i >= 0 && tr[i].end == 0 && tslice->time >= tr[i].start) {
		// active time is within or after current range
		if (tslice->time <= agg0->times.end + agg0->period) {
		    // Common case: active time is within current (unterminated)
		    // range and is no more than 1 step past the last flush.
		    // No change to timerange.

		} else {
		    // Active time is more than 1 step past last flush.
		    // We must terminate the old current range and add a new
		    // current range.
		    tr_changed++;
		    debugUKI(1, "post-current", i); // XXX
		    tr[i].end = agg0->times.end;
		    debugUKI(0, "post-current", i); // XXX
		    if (!(tr = timerange_insert(dkip, ++i, tslice->time, 0)))
			return -1;
		    debugUKI(0, "post-current", i); // XXX
		}

	    } else {
		// search for historic timerange
		while (i >= 0 && tr[i].start > tslice->time) i--;
		if (i >= 0 && tslice->time <= tr[i].end) {
		    // historic timerange matches
		    // debugUKI(1, "historic", i); // XXX
		    // No change to timerange.
		} else if (i >= 0 && tslice->time == tr[i].end + agg0->period) {
		    // time abuts end of historic timerange; extend it
		    tr_changed++;
		    debugUKI(1, "abut end", i); // XXX
		    tr[i].end = tslice->time;
		    debugUKI(0, "abut end", i); // XXX
		    if (i+1 < dkip->n_timeranges &&
			tr[i].end + agg0->period == tr[i+1].start)
		    {
			// extended range i now abuts range i+1; merge them
			int n = --dkip->n_timeranges;
			size_t size = n * sizeof(dbats_timerange_t);
			dbats_timerange_t *newtr = emalloc(size, "timeranges");
			if (!newtr) return -1;
			memcpy(&newtr[0], &tr[0], (i+1) * sizeof(dbats_timerange_t));
			memcpy(&newtr[i+1], &tr[i+2], (n-i-1) * sizeof(dbats_timerange_t));
			newtr[i].end = tr[i+1].end;
			free(tr);
			tr = dkip->timeranges = newtr;
			debugUKI(0, "merge", i); // XXX
		    }
		} else if (i+1 < dkip->n_timeranges &&
		    tslice->time + agg0->period == tr[i+1].start)
		{
		    // time abuts start of historic timerange; extend it
		    tr_changed++;
		    i++;
		    debugUKI(1, "abut start", i); // XXX
		    tr[i].start = tslice->time;
		    debugUKI(0, "abut start", i); // XXX
		} else {
		    // no match found; create a new timerange
		    tr_changed++;
		    if (!(tr = timerange_insert(dkip, ++i, tslice->time,
			(!is_last || next_is_far) ? tslice->time : 0)))
			    return -1;
		    debugUKI(1, "create", i); // XXX
		}
	    }

	} else {
	    // Value is not set.
	    if (dkip->n_timeranges == 0) {
		// No change to timeranges.
	    } else if (tr[i].end == 0 && tslice->time >= tr[i].start) {
		// current timerange matches; terminate it
		tr_changed++;
		debugUKI(1, "unset current", i); // XXX
		tr[i].end = agg0->times.end;
		debugUKI(0, "unset current", i); // XXX
	    } else {
		// search for historic timerange (possible only if we allow
		// deleting historic values)
		tr_changed++; // Assume there will be a change.
		while (i >= 0 && tr[i].start > tslice->time) i--;
		if (i < 0 || tslice->time > tr[i].end) { // no match found
		    tr_changed--; // No change to timeranges; undo assumption.
		} else if (tr[i].start == tslice->time &&
		    tr[i].end == tslice->time)
		{
		    // timerange would become empty; delete it
		    debugUKI(1, "unset historic clear", i); // XXX
		    int n = --dkip->n_timeranges;
		    memmove(&tr[i], &tr[i+1], (n - i) * sizeof(dbats_timerange_t));
		} else if (tr[i].start == tslice->time) {
		    // delete first step of timerange
		    debugUKI(1, "unset historic start", i); // XXX
		    tr[i].start += agg0->period;
		    debugUKI(0, "unset historic start", i); // XXX
		} else if (tr[i].end == tslice->time) {
		    // delete last step of timerange
		    debugUKI(1, "unset historic end", i); // XXX
		    tr[i].end -= agg0->period;
		    debugUKI(0, "unset historic end", i); // XXX
		} else {
		    // split time range
		    debugUKI(1, "unset historic split", i); // XXX
		    if (!(tr = timerange_insert(dkip, i,
			tr[i].start, tslice->time - agg0->period)))
			    return -1;
		    tr[i+1].start = tslice->time + agg0->period;
		    debugUKI(0, "unset historic split", i); // XXX
		    debugUKI(0, "unset historic split", i+1); // XXX
		}
	    }
	}

	// write modified key info to db
	if (tr_changed) {
	    if (set_key_info(handler, dkip) != 0)
		return -1;
	}
    }
    return 0;
}

/*************************************************************************/

void dbats_close(dbats_handler *handler)
{
    if (!handler->is_open) {
	return;
    }
    handler->is_open = 0;

    if (!handler->cfg.readonly)
	dbats_log(LOG_INFO, "Flushing database changes...");

    update_key_info(handler, 0);
    for (int agg_id = handler->cfg.num_aggs - 1; agg_id >= 0; agg_id--) {
	dbats_flush_tslice(handler, agg_id);
	free(handler->tslice[agg_id]);
	handler->tslice[agg_id] = NULL;
    }

    for (uint32_t key_id = 0; key_id < handler->cfg.num_keys; key_id++) {
	dbats_key_info_t *dkip = find_key(handler, key_id);
	free((void*)dkip->key);
	dkip->key = NULL;
	free(dkip->timeranges);
	dkip->timeranges = NULL;
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
    handler->dbenv->close(handler->dbenv, 0);

    if (handler->state_decompress)
	free(handler->state_decompress);
    if (handler->state_compress)
	free(handler->state_compress);
}

/*************************************************************************/

static const time_t time_base = 259200; // 00:00 on first sunday of 1970, UTC

uint32_t dbats_normalize_time(const dbats_handler *handler, int agg_id, uint32_t *t)
{
    *t -= (*t - time_base) % handler->agg[agg_id].period;
    return *t;
}

/*************************************************************************/

static int load_frag(dbats_handler *handler, uint32_t t, int agg_id,
    uint32_t frag_id)
{
    // load fragment
    fragkey_t dbkey;
    void *value;
    size_t value_len;
    dbkey.time = t;
    dbkey.agg_id = agg_id;
    dbkey.frag_id = frag_id;
    int rc = raw_db_get(handler, handler->dbData, &dbkey, sizeof(dbkey),
	&value, &value_len);
    dbats_log(LOG_INFO, "load_frag t=%u, agg_id=%u, frag_id=%u: "
	"raw_db_get = %d", t, agg_id, frag_id, rc);
    if (rc != 0)
	return 0; // no match

    int compressed = *(uint8_t*)value;
    size_t len = !compressed ? fragsize(handler) :
	qlz_size_decompressed((void*)((uint8_t*)value+1));
    void *ptr = malloc(len);
    if (!ptr) {
	dbats_log(LOG_ERROR, "Can't allocate %zu bytes for frag "
	    "t=%u, agg_id=%u, frag_id=%u", len, t, agg_id, frag_id);
	return -2; // error
    }

    if (compressed) {
	// decompress fragment
	// assert(len == fragsize(handler));
	if (!handler->state_decompress) {
	    handler->state_decompress = emalloc(sizeof(qlz_state_decompress), "qlz_state_decompress");
	    if (!handler->state_decompress) return -2;
	}
	len = qlz_decompress((void*)((uint8_t*)value+1), ptr, handler->state_decompress);
	dbats_log(LOG_INFO, "decompressed frag t=%u, agg_id=%u, frag_id=%u: "
	    "%u -> %u (%.1f%%)",
	    t, agg_id, frag_id, value_len, len, len*100.0/value_len);

    } else {
	// copy fragment
	memcpy(ptr, (uint8_t*)value, len);
	dbats_log(LOG_INFO, "copied frag t=%u, agg_id=%u, frag_id=%u",
	    t, agg_id, frag_id);
    }

    handler->tslice[agg_id]->frag[frag_id] = ptr;

    dbats_tslice *tslice = handler->tslice[agg_id];
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;

    return 0;
}

int dbats_goto_time(dbats_handler *handler, uint32_t time_value, uint32_t flags)
{
    int rc;

    dbats_log(LOG_INFO, "goto_time %u", time_value);

    for (int agg_id = 0; agg_id < handler->cfg.num_aggs; agg_id++) {
	dbats_tslice *tslice = handler->tslice[agg_id];
	uint32_t t = time_value;

	dbats_normalize_time(handler, agg_id, &t);
	if (tslice->time == t) {
	    dbats_log(LOG_INFO, "goto_time %u, agg_id=%d: already loaded", t, agg_id);
	    continue;
	}

	if (agg_id == 0) {
	    int next_is_far = t > tslice->time + handler->agg[0].period;
	    if (update_key_info(handler, next_is_far) != 0)
		return -1;
	}

	// Flush tslice if loaded
	dbats_flush_tslice(handler, agg_id);

	tslice->preload = !!(flags & DBATS_PRELOAD);
	tslice->time = t;

	if (!tslice->preload)
	    continue;

	int loaded = 0;
	tslice->num_frags = (handler->cfg.num_keys + ENTRIES_PER_FRAG - 1) /
	    ENTRIES_PER_FRAG;
	for (uint32_t frag_id = 0; frag_id < tslice->num_frags; frag_id++) {
	    if ((rc = load_frag(handler, t, agg_id, frag_id)) != 0)
		return rc;
	    if (tslice->frag[frag_id])
		loaded++;
	}
	dbats_log(LOG_INFO, "goto_time %u: agg_id=%d, loaded %u/%u fragments",
	    time_value, agg_id, loaded, tslice->num_frags);
    }
    return 0;
}

/*************************************************************************/

int dbats_get_key_id(dbats_handler *handler, const char *key,
    uint32_t *key_id_p, uint32_t flags)
{
    void *ptr;
    size_t keylen = strlen(key);
    size_t len;

    if (raw_db_get(handler, handler->dbKeys, key, keylen, &ptr, &len) == 0) {
	*key_id_p = ((raw_key_info_t*)ptr)->key_id;
	// dbats_key_info_t *dkip = find_key(handler, key_id);
	// assert(strcmp(dkip->key, key) == 0);
	dbats_log(LOG_INFO, "Found key #%u: %s", *key_id_p, key);

    } else if (!(flags & DBATS_CREATE)) {
	dbats_log(LOG_INFO, "Key not found: %s", key);
	return -1;

    } else if (handler->cfg.num_keys < MAX_NUM_KEYINFOBLK * KEYINFO_PER_BLK) {
	*key_id_p = handler->cfg.num_keys++;
	dbats_key_info_t *dkip = new_key(handler, *key_id_p, strdup(key));
	if (!dkip) return -1;
	if (set_key_info(handler, dkip) != 0)
	    return -1;
	dbats_log(LOG_INFO, "Assigned key #%d: %s", *key_id_p, key);
	raw_db_set(handler, handler->dbMeta,
	    "num_keys", strlen("num_keys"),
	    &handler->cfg.num_keys, sizeof(handler->cfg.num_keys));

    } else {
	dbats_log(LOG_ERROR, "Out of space for key %s", key);
	return -2;
    }

    return 0;
}

const char *dbats_get_key_name(dbats_handler *handler, uint32_t key_id)
{
    if (key_id >= handler->cfg.num_keys) return NULL;
    dbats_key_info_t *dkip = find_key(handler, key_id);
    return dkip ? dkip->key : NULL;
}

static int instantiate_frag_func(dbats_handler *handler, int agg_id, uint32_t frag_id)
{
    dbats_tslice *tslice = handler->tslice[agg_id];

    if (!tslice->preload || handler->cfg.readonly) {
	// Try to load the fragment.
	// TODO: optionally, unload other fragments?
	int rc = load_frag(handler, tslice->time, agg_id, frag_id);
	if (rc != 0) // error
	    return rc;
	if (tslice->frag[frag_id]) // found it
	    return 0;
	if (handler->cfg.readonly) // didn't find it and can't create it
	    return 1; // fragment not found
    }

    // Allocate a new fragment.
    dbats_log(LOG_NORMAL, "grow tslice for time %u, agg_id %d, frag_id %u",
	tslice->time, agg_id, frag_id);
    tslice->frag[frag_id] = calloc(1, fragsize(handler));
    if (!tslice->frag[frag_id]) {
	dbats_log(LOG_WARNING, "Can't allocate %u bytes to grow tslice "
	    "for time %u, agg_id %d, frag_id %u",
	    fragsize(handler), tslice->time, agg_id, frag_id);
	return -2;
    }
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;
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

static inline uint32_t min(uint32_t a, uint32_t b) { return a < b ? a : b; }
static inline uint32_t max(uint32_t a, uint32_t b) { return a > b ? a : b; }

int dbats_set(dbats_handler *handler, uint32_t key_id, const dbats_value *valuep)
{
    dbats_log(LOG_INFO, "dbats_set %u #%u = %" PRIval, handler->tslice[0]->time, key_id, valuep[0]); // XXX
    int rc;

    dbats_key_info_t *dkip = find_key(handler, key_id);

    if (!handler->is_open || handler->cfg.readonly)
	return -1;
    if ((rc = instantiate_frag(handler, 0, dkip->frag_id)) != 0) {
	handler->cfg.readonly = 1;
	return rc;
    }

    uint8_t was_set = vec_test(handler->tslice[0]->frag[dkip->frag_id]->is_set, dkip->offset);
    dbats_value *oldvaluep = valueptr(handler, 0, dkip->frag_id, dkip->offset);

    for (int agg_id = 1; agg_id < handler->cfg.num_aggs; agg_id++) {
	if ((rc = instantiate_frag(handler, agg_id, dkip->frag_id)) != 0) {
	    handler->cfg.readonly = 1;
	    return rc;
	}

	// Aggregate *valuep into tslice[agg_id]
	uint8_t changed = 0;
	uint8_t failed = 0;
	dbats_value *aggval = valueptr(handler, agg_id, dkip->frag_id, dkip->offset);

	uint32_t aggstart = handler->tslice[agg_id]->time;
	uint32_t aggend = aggstart + (handler->agg[agg_id].steps - 1) * handler->agg[0].period;

	int n = 0; // number of steps contributing to aggregate
	int active_included = 0;
	dbats_timerange_t *tr = dkip->timeranges; // shorthand
	int ntr = dkip->n_timeranges; // number of time ranges
	for (int tri = ntr - 1;
	    tri >= 0 && (tr[tri].end == 0 || tr[tri].end >= aggstart);
	    tri--)
	{
	    if (tr[tri].start > aggend) // no overlap
		continue; 
	    // timerange overlaps aggregate; count overlapping steps
	    uint32_t overlap_start = max(aggstart, tr[tri].start);
	    uint32_t tr_end = tr[tri].end != 0 ?
		tr[tri].end : handler->agg[0].times.end;
	    uint32_t overlap_end = min(aggend, tr_end);
	    // Note:  Usually (overlap_end >= overlap_start), but in the first
	    // tslice of agg period, (overlap_end == overlap_start - period),
	    // so be careful with unsigned underflow.
	    n += (overlap_end + handler->agg[0].period - overlap_start) / handler->agg[0].period;
	    if (handler->tslice[0]->time >= overlap_start &&
		handler->tslice[0]->time <= overlap_end)
		    active_included = 1;
	}
	if (!active_included)
	    n++;

	dbats_log(LOG_INFO, "agg %d: [%u..%u] aggval=%" PRIval " n=%d",
	    agg_id, aggstart, aggend, aggval[0], n); // XXX

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
		    failed = 1;
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
		    failed = 1;
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
		    // find last timerange that overlaps this agg
                    int tri = dkip->n_timeranges - 1;
		    while (tri >= 0 && dkip->timeranges[tri].start > aggend)
			tri--;
		    if (tri < 0) {
			// no timerange overlaps agg
			aggval[i] = valuep[i];
			changed = 1;
		    } else {
			uint32_t tr_end = tr[tri].end != 0 ?
			    tr[tri].end : handler->agg[0].times.end;
			uint32_t overlap_end = min(aggend, tr_end);
			if (handler->tslice[0]->time >= overlap_end) {
			    // no timerange overlaps agg, or active time >= overlap_end
			    aggval[i] = valuep[i];
			    changed = 1;
			}
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

	if (changed) {
	    // XXX if (n >= xff * steps)
	    vec_set(handler->tslice[agg_id]->frag[dkip->frag_id]->is_set, dkip->offset);
	    handler->tslice[agg_id]->frag_changed[dkip->frag_id] = 1;
	    if (handler->agg[agg_id].func == DBATS_AGG_AVG) {
		dbats_log(LOG_INFO, "Succesfully set value "
		    "[agg_id=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u] aggval=%f",
		    agg_id, dkip->frag_id, dkip->offset, handler->cfg.entry_size, *(double*)aggval);
	    } else {
		dbats_log(LOG_INFO, "Succesfully set value "
		    "[agg_id=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u] aggval=%" PRIval,
		    agg_id, dkip->frag_id, dkip->offset, handler->cfg.entry_size, aggval[0]);
	    }
	} else if (failed) {
	    if (handler->agg[agg_id].func == DBATS_AGG_AVG) {
		dbats_log(LOG_ERROR, "Failed to set value "
		    "[agg_id=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u] aggval=%f",
		    agg_id, dkip->frag_id, dkip->offset, handler->cfg.entry_size, *(double*)aggval);
	    } else {
		dbats_log(LOG_ERROR, "Failed to set value "
		    "[agg_id=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u] aggval=%" PRIval,
		    agg_id, dkip->frag_id, dkip->offset, handler->cfg.entry_size, aggval[0]);
	    }
	    handler->cfg.readonly = 1;
	    return -1;
	}
    }

    // Set value at agg_id 0 (after aggregations because aggregations need
    // both old and new values)
    memcpy(oldvaluep, valuep, handler->cfg.entry_size);
    vec_set(handler->tslice[0]->frag[dkip->frag_id]->is_set, dkip->offset);
    handler->tslice[0]->frag_changed[dkip->frag_id] = 1;
    dbats_log(LOG_INFO, "Succesfully set value "
	"[agg_id=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u] value=%" PRIval,
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
	if (!vec_test(tslice->frag[dkip->frag_id]->is_set, dkip->offset)) {
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, agg_id, dkip->key);
	    *valuepp = NULL;
	    return 1;
	}
	if (handler->agg[agg_id].func == DBATS_AGG_AVG) {
	    double *dval = (double*)valueptr(handler, agg_id, dkip->frag_id, dkip->offset);
	    static dbats_value *avg_buf = NULL;
	    if (!avg_buf) {
		avg_buf = emalloc(handler->cfg.entry_size * handler->cfg.values_per_entry, "avg_buf");
		if (!avg_buf) return -2;
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
	if (rc == 1)
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
	if (!vec_test(tslice->frag[dkip->frag_id]->is_set, dkip->offset)) {
	    dbats_log(LOG_WARNING, "Value unset (v): %u %d %s",
		tslice->time, agg_id, dkip->key);
	    *valuepp = NULL;
	    return 1;
	}
	*valuepp = (double*)valueptr(handler, agg_id, dkip->frag_id, dkip->offset);
	dbats_log(LOG_INFO, "Succesfully read value [offset=%" PRIu32 "][value_len=%u]",
	    dkip->offset, handler->cfg.entry_size);

    } else {
	if (rc == 1)
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

    if ((rc = handler->dbKeys->cursor(handler->dbKeys, NULL, &handler->keyname_cursor, 0)) != 0) {
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

const dbats_config *dbats_get_config(dbats_handler *handler)
    { return &handler->cfg; }

const dbats_agg *dbats_get_agg(dbats_handler *handler, int agg_id)
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
}

