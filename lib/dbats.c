/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lessed General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 */

#include <db.h>
#include "tsdb_api.h"


/*************************************************************************/

// bit vector
#define vec_size(N)        (((N)+7)/8)                    // size for N bits
#define vec_set(vec, i)    (vec[(i)/8] |= (1<<((i)%8)))   // set the ith bit
#define vec_reset(vec, i)  (vec[(i)/8] &= ~(1<<((i)%8)))  // reset the ith bit
#define vec_test(vec, i)   (vec[(i)/8] & (1<<((i)%8)))    // test the ith bit

#define valueptr(handler, agg_id, frag_id, offset) \
    ((tsdb_value*)(&handler->tslice[agg_id].frag[frag_id]->data[offset * handler->entry_size]))

struct tsdb_frag {
    uint8_t is_set[vec_size(ENTRIES_PER_FRAG)]; // which indexes have values
    uint8_t data[]; // values (C99 flexible array member)
};

typedef struct {
    uint32_t time;
    int agg_id;
    uint32_t frag_id;
} fragkey_t;

#define fragsize(handler) (sizeof(tsdb_frag) + ENTRIES_PER_FRAG * (handler)->entry_size)

// key_info as stored in db
typedef struct {
    uint32_t index;
    timerange_t timeranges[]; // When did this key have a value? (C99 flexible array member)
} raw_key_info_t;


/************************************************************************
 * utilities
 ************************************************************************/

static void *emalloc(size_t size, const char *msg)
{
    void *ptr = malloc(size);
    if (!ptr)
	traceEvent(TRACE_ERROR, "Can't allocate %u bytes for %s", size, msg);
    return ptr;
}

/************************************************************************
 * DB wrappers
 ************************************************************************/

static int raw_db_open(tsdb_handler *handler, DB **dbp, const char *envpath,
    const char *dbname, uint32_t flags, int mode)
{
    int ret;

    if ((ret = db_create(dbp, handler->dbenv, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error creating DB handler %s: %s",
	    dbname, db_strerror(ret));
	return -1;
    }

    uint32_t dbflags = 0;
    if (flags & TSDB_CREATE)
	dbflags |= DB_CREATE;
    if (flags & TSDB_READONLY)
	dbflags |= DB_RDONLY;

    ret = (*dbp)->open(*dbp, NULL, dbname, NULL, DB_BTREE, dbflags, mode);
    if (ret != 0) {
	traceEvent(TRACE_ERROR, "Error opening DB %s/%s (mode=%o): %s",
	    envpath, dbname, mode, db_strerror(ret));
	return -1;
    }
    return 0;
}

static void raw_db_set(const tsdb_handler *handler, DB *db,
    const void *key, uint32_t key_len,
    const void *value, uint32_t value_len)
{
    DBT key_data, data;
    int ret;

    if (handler->readonly) {
	const char *dbname;
	db->get_dbname(db, NULL, &dbname);
	traceEvent(TRACE_WARNING, "Unable to set value in database \"%s\" "
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
	traceEvent(TRACE_WARNING, "Error in raw_db_set: %s", db_strerror(ret));
}

#define QLZ_OVERHEAD 400

static uint8_t *db_get_buf = 0;
static uint16_t db_get_buf_len = 0;

static int raw_db_get(const tsdb_handler *handler, DB* db,
    const void *key, uint32_t key_len, void **value, uint32_t *value_len)
{
    DBT key_data, data;

    if (db_get_buf_len < ENTRIES_PER_FRAG * handler->entry_size + QLZ_OVERHEAD) {
	db_get_buf_len = ENTRIES_PER_FRAG * handler->entry_size + QLZ_OVERHEAD;
	if (db_get_buf) free(db_get_buf);
	db_get_buf = emalloc(db_get_buf_len, "decompression buffer");
	if (!db_get_buf) return -2;
    }

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));

    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    key_data.flags = DB_DBT_USERMEM;
    data.data = db_get_buf;
    data.ulen = db_get_buf_len;
    data.flags = DB_DBT_USERMEM;
    if (db->get(db, NULL, &key_data, &data, 0) == 0) {
	*value = data.data;
	*value_len = data.size;
	return(0);
    } else {
	//int len = key_len;
	//traceEvent(TRACE_WARNING, "raw_db_get failed: key=\"%.*s\"\n", len, (char*)key);
	return -1;
    }
}

/*
static void raw_db_delete(const tsdb_handler *handler, DB *db,
    const void *key, uint32_t key_len)
{
    DBT key_data;

    if (handler->readonly) {
	traceEvent(TRACE_WARNING, "Unable to delete value (read-only mode)");
	return;
    }

    memset(&key_data, 0, sizeof(key_data));
    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    key_data.flags = DB_DBT_USERMEM;

    if (db->del(db, NULL, &key_data, 0) != 0)
	traceEvent(TRACE_WARNING, "Error while deleting key");
}
*/

/*
static int raw_db_key_exists(const tsdb_handler *handler, DB *db,
    const void *key, uint32_t key_len)
{
    void *value;
    uint32_t value_len;

    return (raw_db_get(handler, db, key, key_len, &value, &value_len) == 0);
}
*/

/*************************************************************************/

// allocate a key_info block if needed
static int alloc_key_info_block(tsdb_handler *handler, uint32_t block)
{
    if (!handler->key_info_block[block]) {
	handler->key_info_block[block] =
	    emalloc(sizeof(tsdb_key_info_t) * INFOS_PER_BLOCK, "key_info_block");
	if (!handler->key_info_block[block])
	    return -1;
    }
    return 0;
}

int tsdb_open(tsdb_handler *handler, const char *path,
    uint16_t num_values_per_entry,
    uint32_t period,
    uint32_t flags)
{
    int ret;
    int dbmode = 00666;
    uint32_t dbflags = DB_INIT_MPOOL;

    memset(handler, 0, sizeof(tsdb_handler));
    handler->readonly = !!(flags & TSDB_READONLY);
    handler->num_values_per_entry = 1;

    if ((ret = db_env_create(&handler->dbenv, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error creating DB env: %s",
	    db_strerror(ret));
	return -1;
    }

    if (flags & TSDB_CREATE) {
	if (mkdir(path, 0777) < 0 && errno != EEXIST) {
	    traceEvent(TRACE_ERROR, "Error creating %s: %s",
		path, strerror(errno));
	    return -1;
	}
	dbflags |= DB_CREATE;
    }

    if ((ret = handler->dbenv->open(handler->dbenv, path, dbflags, dbmode)) != 0) {
	traceEvent(TRACE_ERROR, "Error opening DB environment: %s",
	    db_strerror(ret));
	return -1;
    }

    if (raw_db_open(handler, &handler->dbMeta, path, "meta", flags, dbmode) != 0) return -1;
    if (raw_db_open(handler, &handler->dbKeys, path, "keys", flags, dbmode) != 0) return -1;
    if (raw_db_open(handler, &handler->dbData, path, "data", flags, dbmode) != 0) return -1;

    handler->entry_size = sizeof(tsdb_value); // for db_get_buf_len

#define initcfg(handler, type, field, defaultval) \
    do { \
	void *value; \
	uint32_t value_len; \
	if (raw_db_get(handler, handler->dbMeta, #field, strlen(#field), &value, &value_len) == 0) { \
	    handler->field = *((type*)value); \
	} else if (!handler->readonly) { \
	    handler->field = (defaultval); \
	    raw_db_set(handler, handler->dbMeta, #field, strlen(#field), \
		&handler->field, sizeof(handler->field)); \
	} \
	traceEvent(TRACE_INFO, "cfg: %-.22s = %u", #field, handler->field); \
    } while (0)

    initcfg(handler, uint32_t, lowest_free_index,      0);
    initcfg(handler, uint32_t, period,                 period);
    initcfg(handler, uint16_t, num_values_per_entry,   num_values_per_entry);
    initcfg(handler, uint16_t, num_aggs,            1);

    if (handler->num_aggs > MAX_NUM_AGGS) {
	traceEvent(TRACE_ERROR, "num_aggs %d > %d", handler->num_aggs,
	    MAX_NUM_AGGS);
	handler->readonly = 1;
	return -1;
    }

    handler->entry_size = handler->num_values_per_entry * sizeof(tsdb_value);

    memset(&handler->state_compress, 0, sizeof(handler->state_compress));
    memset(&handler->state_decompress, 0, sizeof(handler->state_decompress));

    handler->agg[0].func = TSDB_AGG_NONE;
    handler->agg[0].steps = 1;
    handler->agg[0].period = handler->period;

    // load metadata for aggregates
    if (handler->num_aggs > 1) {
	void *value;
	uint32_t value_len;
	if (raw_db_get(handler, handler->dbMeta, "agg", strlen("agg"), &value, &value_len) == 0) {
	    if (value_len != handler->num_aggs * sizeof(tsdb_agg)) {
		traceEvent(TRACE_ERROR, "corrupt aggregation config %d != %d",
		    value_len, handler->num_aggs * sizeof(tsdb_agg));
		handler->readonly = 1;
		return -1;
	    }
	}
	memcpy(handler->agg, value, value_len);
    }

    // load key_info for every key
    int rc;
    DBC *cursor;
    if ((rc = handler->dbKeys->cursor(handler->dbKeys, NULL, &cursor, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error in keywalk_start: %s", db_strerror(rc));
	return -1;
    }

    while (1) {
	DBT dbkey, dbdata;

	memset(&dbkey, 0, sizeof(dbkey));
	memset(&dbdata, 0, sizeof(dbdata));
	if ((rc = cursor->get(cursor, &dbkey, &dbdata, DB_NEXT)) != 0) {
	    if (rc == DB_NOTFOUND)
		break;
	    traceEvent(TRACE_ERROR, "Error in cursor->get: %s", db_strerror(rc));
	    return -1;
	}
	uint32_t idx = ((raw_key_info_t*)dbdata.data)->index;
	uint32_t block = idx / INFOS_PER_BLOCK;
	uint32_t offset = idx % INFOS_PER_BLOCK;
	if (alloc_key_info_block(handler, block) != 0)
	    return -1;

	tsdb_key_info_t *tkip = &handler->key_info_block[block][offset];
	raw_key_info_t *rki = dbdata.data;
	char *keycopy = emalloc(dbkey.size+1, "copy of key");
	if (!keycopy) return -2;
	memcpy(keycopy, dbkey.data, dbkey.size);
	keycopy[dbkey.size] = 0; // nul-terminate
	tkip->key = keycopy;
	tkip->index = rki->index;
	tkip->frag_id = tkip->index / ENTRIES_PER_FRAG;
	tkip->offset = tkip->index % ENTRIES_PER_FRAG;
	size_t tr_size = dbdata.size - sizeof(raw_key_info_t);
	tkip->n_timeranges = tr_size / sizeof(timerange_t);
	tkip->timeranges = emalloc(tr_size, "timeranges");
	if (!tkip->timeranges) return -2;
	memcpy(tkip->timeranges, rki->timeranges, tr_size);
    }

    if ((rc = cursor->close(cursor)) != 0) {
	traceEvent(TRACE_ERROR, "Error in cursor->close: %s", db_strerror(rc));
	return -1;
    }

    handler->is_open = 1;

    return(0);
}

int tsdb_aggregate(tsdb_handler *handler, int func, int steps)
{
    if (handler->tslice[0].time > 0) {
	// XXX fix this?
	traceEvent(TRACE_ERROR,
	    "Adding a new aggregation to existing data is not yet supported.");
	return -1;
    }

    if (handler->num_aggs >= MAX_NUM_AGGS) {
	traceEvent(TRACE_ERROR, "Too many aggregations (%d)", MAX_NUM_AGGS);
	return -1;
    }

    int agg_id = handler->num_aggs++;
    handler->agg[agg_id].func = func;
    handler->agg[agg_id].steps = steps;
    handler->agg[agg_id].period = handler->agg[0].period * steps;

    raw_db_set(handler, handler->dbMeta, "agg", strlen("agg"),
	handler->agg, sizeof(tsdb_agg) * handler->num_aggs);
    raw_db_set(handler, handler->dbMeta, "num_aggs", strlen("num_aggs"),
	&handler->num_aggs, sizeof(handler->num_aggs));

    return(0);
}

/*************************************************************************/

// Writes tslice to db (unless db is readonly).
// Also cleans up in-memory tslice, even if db is readonly.
static void tsdb_flush_tslice(tsdb_handler *handler, int agg_id)
{
    traceEvent(TRACE_INFO, "Flush t=%u agg_id=%d", handler->tslice[agg_id].time, agg_id);
    uint32_t frag_size = fragsize(handler);
    fragkey_t dbkey;
    uint32_t buf_len = frag_size + QLZ_OVERHEAD;
    char *buf = NULL;


    // Write fragments to the DB
    dbkey.time = handler->tslice[agg_id].time;
    dbkey.agg_id = agg_id;
    for (int f = 0; f < handler->tslice[agg_id].num_frags; f++) {
	if (!handler->tslice[agg_id].frag[f]) continue;
	if (handler->readonly) {
	    // do nothing
	} else if (handler->tslice[agg_id].frag_changed[f]) {
	    if (!buf)
		buf = (char*)emalloc(buf_len, "compression buffer");
	    if (!buf) return;
	    size_t compressed_len = qlz_compress(handler->tslice[agg_id].frag[f],
		buf, frag_size, &handler->state_compress);
	    traceEvent(TRACE_INFO, "Frag %d compression: %u -> %u (%.1f %%)",
		f, frag_size, compressed_len, compressed_len*100.0/frag_size);
	    dbkey.frag_id = f;
	    raw_db_set(handler, handler->dbData, &dbkey, sizeof(dbkey), buf, compressed_len);
	} else {
	    traceEvent(TRACE_INFO, "Skipping frag %d (unchanged)", f);
	}

	handler->tslice[agg_id].frag_changed[f] = 0;
	free(handler->tslice[agg_id].frag[f]);
	handler->tslice[agg_id].frag[f] = 0;
    }

    if (!handler->readonly) {
	if (handler->agg[agg_id].last_flush < handler->tslice[agg_id].time)
	    handler->agg[agg_id].last_flush = handler->tslice[agg_id].time;
	raw_db_set(handler, handler->dbMeta, "agg", strlen("agg"),
	    handler->agg, sizeof(tsdb_agg) * handler->num_aggs);
    }

    memset(&handler->tslice[agg_id], 0, sizeof(tsdb_tslice));
    if (buf) free(buf);
}

/*************************************************************************/

static int set_key_info(const tsdb_handler *handler, tsdb_key_info_t *tkip)
{
    size_t tr_size = tkip->n_timeranges * sizeof(timerange_t);
    size_t rki_size = sizeof(raw_key_info_t) + tr_size;
    raw_key_info_t *rki = emalloc(rki_size, tkip->key);
    if (!rki) return -1;
    rki->index = tkip->index;
    memcpy(rki->timeranges, tkip->timeranges, tr_size);
    raw_db_set(handler, handler->dbKeys, tkip->key, strlen(tkip->key), rki, rki_size);
    free(rki);
    traceEvent(TRACE_INFO, "Write key %s -> %u, %d timeranges",
	tkip->key, tkip->index, tkip->n_timeranges);
    /*
    int totalsteps = 0; // XXX
    for (int i = 0; i < tkip->n_timeranges; i++) { // XXX
	int steps = (tkip->timeranges[i].end + handler->agg[0].period - tkip->timeranges[i].start) /
	    handler->agg[0].period;
	traceEvent(TRACE_INFO, "     %3d: %10u %10u (%3d)",
	    i, tkip->timeranges[i].start, tkip->timeranges[i].end, steps);
	totalsteps += steps;
    }
    traceEvent(TRACE_INFO, "                              %6d", totalsteps); // XXX
    */
    return 0;
}

// Insert a new timerange into tkip->timeranges at position i, and initialize it
static timerange_t *timerange_insert(tsdb_key_info_t *tkip, int i,
    uint32_t start, uint32_t end)
{
    int n = ++tkip->n_timeranges;
    timerange_t *tr = tkip->timeranges;
    timerange_t *newtr = emalloc(n * sizeof(timerange_t), "timeranges");
    if (!newtr) return NULL;
    if (tr) {
	memcpy(&newtr[0], &tr[0], i * sizeof(timerange_t));
	memcpy(&newtr[i+1], &tr[i], (n-1-i) * sizeof(timerange_t));
	free(tr);
    }
    newtr[i].start = start;
    newtr[i].end = end;
    return tkip->timeranges = newtr;
}

// Update the timeranges associated with each key.
static int update_key_info(tsdb_handler *handler, int next_is_far)
{
    tsdb_agg *agg0 = &handler->agg[0];
    tsdb_tslice *tslice = &handler->tslice[0];
    if (handler->readonly || tslice->time == 0) return 0;
    int is_last = tslice->time >= agg0->last_flush;
    traceEvent(TRACE_INFO,
	"update_key_info: t=%u, last_flush=%u, is_last=%d, next_is_far=%d",
	tslice->time, agg0->last_flush, is_last, next_is_far); // XXX

#define debugUKI(first, label, i) \
    traceEvent(TRACE_INFO, \
	"%s idx=%u, t=%u, [%u..%u] %s %d/%d", \
	(first ? "update_key_info:" : "             -->"), \
	idx, tslice->time, tr[i].start, tr[i].end, label, i, tkip->n_timeranges)

    for (uint32_t idx = 0; idx < handler->lowest_free_index; idx++) {
	uint32_t block = idx / INFOS_PER_BLOCK;
	uint32_t offset = idx % INFOS_PER_BLOCK;
	tsdb_key_info_t *tkip = &handler->key_info_block[block][offset];
	timerange_t *tr = tkip->timeranges; // shorthand

	int i = tkip->n_timeranges - 1;
	int tr_changed = 0; // timerange changed?

	// Should timerange change due to next_is_far?

	if (next_is_far && i >= 0 && tr[i].end == 0) {
	    // next time > active time + 1 step (uncommon).
	    // Terminate the current (unterminated) timerange.
	    tr_changed++;
	    debugUKI(1, "next_is_far", i); // XXX
	    tr[i].end = agg0->last_flush;
	    debugUKI(0, "next_is_far", i); // XXX
	}

	// Should timerange change because value's set/unset state changed?

	if (!tslice->frag[tkip->frag_id]) {
	    // Fragment wasn't loaded, so value's state could not have changed.
	    if (tslice->time > agg0->last_flush) {
		// This is the first visit to the current timeslice, so the
		// value in this timeslice could never have been set before.
		if (i >= 0 && tr[i].end == 0) {
		    // Key has a current timerange; we must terminate it.
		    tr_changed++;
		    debugUKI(1, "terminate", i);
		    tr[i].end = agg0->last_flush;
		    debugUKI(0, "terminate", i);
		}
	    }

	} else if (vec_test(tslice->frag[tkip->frag_id]->is_set, tkip->offset)) {
	    // Value is set.
	    if (i >= 0 && tr[i].end == 0 && tslice->time >= tr[i].start) {
		// active time is within or after current range
		if (tslice->time <= agg0->last_flush + agg0->period) {
		    // Common case: active time is within current (unterminated)
		    // range and is no more than 1 step past the last flush.
		    // No change to timerange.

		} else {
		    // Active time is more than 1 step past last flush.
		    // We must terminate the old current range and add a new
		    // current range.
		    tr_changed++;
		    debugUKI(1, "post-current", i); // XXX
		    tr[i].end = agg0->last_flush;
		    debugUKI(0, "post-current", i); // XXX
		    if (!(tr = timerange_insert(tkip, ++i, tslice->time, 0)))
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
		    if (i+1 < tkip->n_timeranges &&
			tr[i].end + agg0->period == tr[i+1].start)
		    {
			// extended range i now abuts range i+1; merge them
			int n = --tkip->n_timeranges;
			size_t size = n * sizeof(timerange_t);
			timerange_t *newtr = emalloc(size, "timeranges");
			if (!newtr) return -1;
			memcpy(&newtr[0], &tr[0], (i+1) * sizeof(timerange_t));
			memcpy(&newtr[i+1], &tr[i+2], (n-i-1) * sizeof(timerange_t));
			newtr[i].end = tr[i+1].end;
			free(tr);
			tr = tkip->timeranges = newtr;
			debugUKI(0, "merge", i); // XXX
		    }
		} else if (i+1 < tkip->n_timeranges &&
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
		    if (!(tr = timerange_insert(tkip, ++i, tslice->time,
			(!is_last || next_is_far) ? tslice->time : 0)))
			    return -1;
		    debugUKI(1, "create", i); // XXX
		}
	    }

	} else {
	    // Value is not set.
	    if (tkip->n_timeranges == 0) {
		// No change to timeranges.
	    } else if (tr[i].end == 0 && tslice->time >= tr[i].start) {
		// current timerange matches; terminate it
		tr_changed++;
		debugUKI(1, "unset current", i); // XXX
		tr[i].end = agg0->last_flush;
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
		    int n = --tkip->n_timeranges;
		    memmove(&tr[i], &tr[i+1], (n - i) * sizeof(timerange_t));
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
		    if (!(tr = timerange_insert(tkip, i,
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
	    if (set_key_info(handler, tkip) != 0)
		return -1;
	}
    }
    return 0;
}

/*************************************************************************/

void tsdb_close(tsdb_handler *handler)
{
    if (!handler->is_open) {
	return;
    }
    handler->is_open = 0;

    if (!handler->readonly)
	traceEvent(TRACE_INFO, "Flushing database changes...");

    update_key_info(handler, 0);
    for (int agg_id = 0; agg_id < handler->num_aggs; agg_id++) {
	tsdb_flush_tslice(handler, agg_id);
    }

    for (uint32_t idx = 0; idx < handler->lowest_free_index; idx++) {
	uint32_t block = idx / INFOS_PER_BLOCK;
	uint32_t offset = idx % INFOS_PER_BLOCK;
	tsdb_key_info_t *tkip = &handler->key_info_block[block][offset];
	free(tkip->key);
	tkip->key = NULL;
	free(tkip->timeranges);
	tkip->timeranges = NULL;
    }
    for (uint32_t block = 0; block < (handler->lowest_free_index + INFOS_PER_BLOCK - 1) / INFOS_PER_BLOCK; block++) {
	free(handler->key_info_block[block]);
	handler->key_info_block[block] = NULL;
    }
    free(db_get_buf);
    db_get_buf = NULL;
    db_get_buf_len = 0;

    //handler->dbMeta->close(handler->dbMeta, 0);
    //handler->dbKeys->close(handler->dbKeys, 0);
    //handler->dbData->close(handler->dbData, 0);
    handler->dbenv->close(handler->dbenv, 0);
}

/*************************************************************************/

static const time_t time_base = 259200; // 00:00 on first sunday of 1970, UTC

uint32_t normalize_time(const tsdb_handler *handler, int agg_id, uint32_t *t)
{
    *t -= (*t - time_base) % handler->agg[agg_id].period;
    return *t;
}

/*************************************************************************/

static int load_frag(tsdb_handler *handler, uint32_t t, int agg_id,
    uint32_t frag_id)
{
    // load fragment
    fragkey_t dbkey;
    void *value;
    uint32_t value_len;
    dbkey.time = t;
    dbkey.agg_id = agg_id;
    dbkey.frag_id = frag_id;
    int rc = raw_db_get(handler, handler->dbData, &dbkey, sizeof(dbkey),
	&value, &value_len);
    traceEvent(TRACE_INFO, "load_frag t=%u, agg_id=%u, frag_id=%u: "
	"raw_db_get = %d", t, agg_id, frag_id, rc);
    if (rc != 0)
	return 0; // no match

    // decompress fragment
    size_t len = qlz_size_decompressed(value);
    // assert(len == fragsize(handler));
    void *ptr = malloc(len);
    if (!ptr) {
	traceEvent(TRACE_ERROR, "Can't allocate %u bytes for frag "
	    "t=%u, agg_id=%u, frag_id=%u", len, t, agg_id, frag_id);
	return -2; // error
    }
    len = qlz_decompress(value, ptr, &handler->state_decompress);
    traceEvent(TRACE_INFO, "decompressed frag t=%u, agg_id=%u, frag_id=%u: "
	"%u -> %u (%.1f%%)",
	t, agg_id, frag_id, value_len, len, len*100.0/value_len);

    handler->tslice[agg_id].frag[frag_id] = ptr;

    tsdb_tslice *tslice = &handler->tslice[agg_id];
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;

    return 0;
}

int tsdb_goto_time(tsdb_handler *handler, uint32_t time_value, uint32_t flags)
{
    int rc;

    traceEvent(TRACE_INFO, "goto_time %u", time_value);

    for (int agg_id = 0; agg_id < handler->num_aggs; agg_id++) {
	tsdb_tslice *tslice = &handler->tslice[agg_id];
	uint32_t t = time_value;

	normalize_time(handler, agg_id, &t);
	if (tslice->time == t) {
	    traceEvent(TRACE_INFO, "goto_time %u, agg_id=%d: already loaded", t, agg_id);
	    continue;
	}

	if (agg_id == 0) {
	    int next_is_far = t > tslice->time + handler->agg[0].period;
	    if (update_key_info(handler, next_is_far) != 0)
		return -1;
	}

	// Flush tslice if loaded
	tsdb_flush_tslice(handler, agg_id);

	tslice->load_on_demand = !!(flags & TSDB_LOAD_ON_DEMAND);
	tslice->time = t;

	if (tslice->load_on_demand)
	    continue;

	int loaded = 0;
	tslice->num_frags = (handler->lowest_free_index + ENTRIES_PER_FRAG - 1) /
	    ENTRIES_PER_FRAG;
	for (uint32_t frag_id = 0; frag_id < tslice->num_frags; frag_id++) {
	    if ((rc = load_frag(handler, t, agg_id, frag_id)) != 0)
		return rc;
	    if (tslice->frag[frag_id])
		loaded++;
	}
	traceEvent(TRACE_INFO, "goto_time %u: agg_id=%d, loaded %u/%u fragments",
	    time_value, agg_id, loaded, tslice->num_frags);
    }
    return 0;
}

/*************************************************************************/

int tsdb_get_key_info(tsdb_handler *handler, const char *key,
    tsdb_key_info_t **tkipp, uint32_t flags)
{
    void *ptr;
    uint32_t keylen = strlen(key);
    uint32_t len;

    if (raw_db_get(handler, handler->dbKeys, key, keylen, &ptr, &len) == 0) {
	uint32_t idx = ((raw_key_info_t*)ptr)->index;
	uint32_t block = idx / INFOS_PER_BLOCK;
	uint32_t offset = idx % INFOS_PER_BLOCK;
	*tkipp = &handler->key_info_block[block][offset];
	// assert(strcmp((*tkipp)->key, key) == 0);
	traceEvent(TRACE_INFO, "Found key %s = index %u", key, idx);

    } else if (!(flags & TSDB_CREATE)) {
	traceEvent(TRACE_INFO, "Key not found: %s", key);
	return -1;

    } else if (handler->lowest_free_index < MAX_NUM_FRAGS * ENTRIES_PER_FRAG) {
	uint32_t idx = handler->lowest_free_index++;
	uint32_t block = idx / INFOS_PER_BLOCK;
	uint32_t offset = idx % INFOS_PER_BLOCK;
	if (alloc_key_info_block(handler, block) != 0)
	    return -1;
	*tkipp = &handler->key_info_block[block][offset];
	(*tkipp)->key = strdup(key);
	(*tkipp)->index = idx;
	(*tkipp)->frag_id = (*tkipp)->index / ENTRIES_PER_FRAG;
	(*tkipp)->offset = (*tkipp)->index % ENTRIES_PER_FRAG;
	(*tkipp)->n_timeranges = 0;
	(*tkipp)->timeranges = NULL;
	if (set_key_info(handler, *tkipp) != 0)
	    return -1;
	traceEvent(TRACE_INFO, "Assigned key %s = index %u", key, (*tkipp)->index);
	raw_db_set(handler, handler->dbMeta,
	    "lowest_free_index", strlen("lowest_free_index"),
	    &handler->lowest_free_index, sizeof(handler->lowest_free_index));

    } else {
	traceEvent(TRACE_ERROR, "Out of indexes for key %s", key);
	return -2;
    }

    return 0;
}

static int instantiate_frag(tsdb_handler *handler, int agg_id, uint32_t frag_id)
{
    tsdb_tslice *tslice = &handler->tslice[agg_id];

    if (frag_id > MAX_NUM_FRAGS) {
	traceEvent(TRACE_ERROR, "Internal error: frag_id %u > %u",
	    frag_id, MAX_NUM_FRAGS);
	return -2;
    }

    if (tslice->frag[frag_id]) {
	// We already have the right fragment.  Do nothing.
	return 0;
    }

    if (tslice->load_on_demand || handler->readonly) {
	// Try to load the fragment.
	// TODO: optionally, unload other fragments?
	int rc = load_frag(handler, tslice->time, agg_id, frag_id);
	if (rc != 0) // error
	    return rc;
	if (tslice->frag[frag_id]) // found it
	    return 0;
	if (handler->readonly) // didn't find it and can't create it
	    return 1; // fragment not found
    }

    // Allocate a new fragment.
    traceEvent(TRACE_NORMAL, "grow tslice for time %u, agg_id %d, frag_id %u",
	tslice->time, agg_id, frag_id);
    tslice->frag[frag_id] = calloc(1, fragsize(handler));
    if (!tslice->frag[frag_id]) {
	traceEvent(TRACE_WARNING, "Can't allocate %u bytes to grow tslice "
	    "for time %u, agg_id %d, frag_id %u",
	    fragsize(handler), tslice->time, agg_id, frag_id);
	return -2;
    }
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;
    traceEvent(TRACE_INFO, "Grew tslice to %u elements",
	tslice->num_frags * ENTRIES_PER_FRAG);

    return 0;
}

/*************************************************************************/

static inline uint32_t min(uint32_t a, uint32_t b) { return a < b ? a : b; }

static inline uint32_t max(uint32_t a, uint32_t b) { return a > b ? a : b; }

int tsdb_set(tsdb_handler *handler, tsdb_key_info_t *tkip, const tsdb_value *valuep)
{
    traceEvent(TRACE_INFO, "tsdb_set %u %u = %u", handler->tslice[0].time, tkip->index, valuep[0]); // XXX
    int rc;

    if (!handler->is_open || handler->readonly)
	return -1;
    if ((rc = instantiate_frag(handler, 0, tkip->frag_id)) != 0) {
	handler->readonly = 1;
	return rc;
    }

    uint8_t was_set = vec_test(handler->tslice[0].frag[tkip->frag_id]->is_set, tkip->offset);
    tsdb_value *oldvaluep = valueptr(handler, 0, tkip->frag_id, tkip->offset);

    for (int agg_id = 1; agg_id < handler->num_aggs; agg_id++) {
	if ((rc = instantiate_frag(handler, agg_id, tkip->frag_id)) != 0) {
	    handler->readonly = 1;
	    return rc;
	}

	// Aggregate *valuep into tslice[agg_id]
	uint8_t changed = 0;
	uint8_t failed = 0;
	tsdb_value *aggval = valueptr(handler, agg_id, tkip->frag_id, tkip->offset);

	uint32_t aggstart = handler->tslice[agg_id].time;
	uint32_t aggend = aggstart + (handler->agg[agg_id].steps - 1) * handler->agg[0].period;

	int n = 0; // number of steps contributing to aggregate
	int active_included = 0;
	timerange_t *tr = tkip->timeranges; // shorthand
	int ntr = tkip->n_timeranges; // number of time ranges
	for (int tri = ntr - 1;
	    tri >= 0 && (tr[tri].end == 0 || tr[tri].end >= aggstart);
	    tri--)
	{
	    if (tr[tri].start > aggend) // no overlap
		continue; 
	    // timerange overlaps aggregate; count overlapping steps
	    uint32_t overlap_start = max(aggstart, tr[tri].start);
	    uint32_t tr_end = tr[tri].end != 0 ?
		tr[tri].end : handler->agg[0].last_flush;
	    uint32_t overlap_end = min(aggend, tr_end);
	    // Note:  Usually (overlap_end >= overlap_start), but in the first
	    // tslice of agg period, (overlap_end == overlap_start - period),
	    // so be careful with unsigned underflow.
	    n += (overlap_end + handler->agg[0].period - overlap_start) / handler->agg[0].period;
	    if (handler->tslice[0].time >= overlap_start &&
		handler->tslice[0].time <= overlap_end)
		    active_included = 1;
	}
	if (!active_included)
	    n++;

	traceEvent(TRACE_INFO, "agg %d: [%u..%u] aggval=%u n=%d",
	    agg_id, aggstart, aggend, aggval[0], n); // XXX

	switch (handler->agg[agg_id].func) {
	case TSDB_AGG_MIN:
	    for (int i = 0; i < handler->num_values_per_entry; i++) {
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
	case TSDB_AGG_MAX:
	    for (int i = 0; i < handler->num_values_per_entry; i++) {
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
	case TSDB_AGG_AVG:
	    for (int i = 0; i < handler->num_values_per_entry; i++) {
		if (was_set && valuep[i] == oldvaluep[i]) {
		    // value did not change; no need to change agg value
		} else if (n == 1) {
		    aggval[i] = valuep[i];
		    changed = 1;
		} else if (aggval[i] != valuep[i]) {
		    if (was_set)
			aggval[i] -= ((/*signed*/double)oldvaluep[i] - aggval[i]) / n + 0.5;
		    aggval[i] += ((/*signed*/double)valuep[i] - aggval[i]) / n + 0.5;
		    changed = 1;
		}
	    }
	    break;
	case TSDB_AGG_LAST:
	    for (int i = 0; i < handler->num_values_per_entry; i++) {
		if (was_set && valuep[i] == oldvaluep[i]) {
		    // value did not change; no need to change agg value
		} else if (handler->tslice[0].time >= handler->agg[0].last_flush) {
		    // common case: value is latest ever seen
		    aggval[i] = valuep[i];
		    changed = 1;
		} else {
		    // find last timerange that overlaps this agg
                    int tri = tkip->n_timeranges - 1;
		    while (tri >= 0 && tkip->timeranges[tri].start > aggend)
			tri--;
		    if (tri < 0) {
			// no timerange overlaps agg
			aggval[i] = valuep[i];
			changed = 1;
		    } else {
			uint32_t tr_end = tr[tri].end != 0 ?
			    tr[tri].end : handler->agg[0].last_flush;
			uint32_t overlap_end = min(aggend, tr_end);
			if (handler->tslice[0].time >= overlap_end) {
			    // no timerange overlaps agg, or active time >= overlap_end
			    aggval[i] = valuep[i];
			    changed = 1;
			}
		    }
		}
	    }
	    break;
	case TSDB_AGG_SUM:
	    for (int i = 0; i < handler->num_values_per_entry; i++) {
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
	    vec_set(handler->tslice[agg_id].frag[tkip->frag_id]->is_set, tkip->offset);
	    handler->tslice[agg_id].frag_changed[tkip->frag_id] = 1;
	    traceEvent(TRACE_INFO, "Succesfully set value "
		"[agg_id=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u] aggval=%u",
		agg_id, tkip->frag_id, tkip->offset, handler->entry_size, aggval[0]);
	} else if (failed) {
	    traceEvent(TRACE_ERROR, "Failed to set value "
		"[agg_id=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u] aggval=%u",
		agg_id, tkip->frag_id, tkip->offset, handler->entry_size, aggval[0]);
	    handler->readonly = 1;
	    return -1;
	}
    }

    // Set value at agg_id 0 (after aggregations because aggregations need
    // both old and new values)
    memcpy(oldvaluep, valuep, handler->entry_size);
    vec_set(handler->tslice[0].frag[tkip->frag_id]->is_set, tkip->offset);
    handler->tslice[0].frag_changed[tkip->frag_id] = 1;
    traceEvent(TRACE_INFO, "Succesfully set value "
	"[agg_id=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u] value=%u",
	0, tkip->frag_id, tkip->offset, handler->entry_size, valuep[0]);

    return 0;
}

int tsdb_set_by_key(tsdb_handler *handler, const char *key,
    const tsdb_value *valuep)
{
    int rc;
    tsdb_key_info_t *tkip;

    if ((rc = tsdb_get_key_info(handler, key, &tkip, TSDB_CREATE)) != 0) {
	handler->readonly = 1;
	return rc;
    }
    return tsdb_set(handler, tkip, valuep);
}

/*************************************************************************/

int tsdb_get(tsdb_handler *handler, tsdb_key_info_t *tkip,
    const tsdb_value **valuepp, int agg_id)
{
    int rc;
    tsdb_tslice *tslice = &handler->tslice[agg_id];

    if (!handler->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if ((rc = instantiate_frag(handler, agg_id, tkip->frag_id)) == 0) {
	if (!vec_test(tslice->frag[tkip->frag_id]->is_set, tkip->offset)) {
	    traceEvent(TRACE_WARNING, "Value unset (v): %u %d %s",
		tslice->time, agg_id, tkip->key);
	    *valuepp = NULL;
	    return 1;
	}
	*valuepp = valueptr(handler, agg_id, tkip->frag_id, tkip->offset);
	traceEvent(TRACE_INFO, "Succesfully read value [offset=%" PRIu32 "][value_len=%u]",
	    tkip->offset, handler->entry_size);

    } else {
	if (rc == 1)
	    traceEvent(TRACE_WARNING, "Value unset (f): %u %d %s",
		tslice->time, agg_id, tkip->key);
	*valuepp = NULL;
	return rc;
    }

    return 0;
}

int tsdb_get_by_key(tsdb_handler *handler, const char *key,
    const tsdb_value **valuepp, int agg_id)
{
    int rc;
    tsdb_key_info_t *tkip;

    if ((rc = tsdb_get_key_info(handler, key, &tkip, 0)) != 0) {
	return rc;
    }
    return tsdb_get(handler, tkip, valuepp, agg_id);
}

/*************************************************************************/

int tsdb_keywalk_start(tsdb_handler *handler)
{
    int rc;

    if ((rc = handler->dbKeys->cursor(handler->dbKeys, NULL, &handler->keywalk, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error in tsdb_keywalk_start: %s", db_strerror(rc));
	return -1;
    }
    return 0;
}

int tsdb_keywalk_next(tsdb_handler *handler, tsdb_key_info_t **tkipp)
{
    int rc;
    DBT dbkey, dbdata;

    memset(&dbkey, 0, sizeof(dbkey));
    memset(&dbdata, 0, sizeof(dbdata));
    if ((rc = handler->keywalk->get(handler->keywalk, &dbkey, &dbdata, DB_NEXT)) != 0) {
	if (rc != DB_NOTFOUND)
	    traceEvent(TRACE_ERROR, "Error in tsdb_keywalk_next: %s", db_strerror(rc));
	*tkipp = NULL;
	return -1;
    }

    uint32_t idx = ((raw_key_info_t*)dbdata.data)->index;
    uint32_t block = idx / INFOS_PER_BLOCK;
    uint32_t offset = idx % INFOS_PER_BLOCK;
    *tkipp = &handler->key_info_block[block][offset];

    return 0;
}

int tsdb_keywalk_end(tsdb_handler *handler)
{
    int rc;

    if ((rc = handler->keywalk->close(handler->keywalk)) != 0) {
	traceEvent(TRACE_ERROR, "Error in tsdb_keywalk_end: %s", db_strerror(rc));
	return -1;
    }
    return 0;
}

/*************************************************************************/

void tsdb_stat_print(const tsdb_handler *handler) {
    int rc;
    
    if ((rc = handler->dbMeta->stat_print(handler->dbMeta, DB_FAST_STAT)) != 0)
	traceEvent(TRACE_ERROR, "Error while dumping DB stats for Meta [%s]", db_strerror(rc));
    if ((rc = handler->dbKeys->stat_print(handler->dbKeys, DB_FAST_STAT)) != 0)
	traceEvent(TRACE_ERROR, "Error while dumping DB stats for Key [%s]", db_strerror(rc));
    if ((rc = handler->dbData->stat_print(handler->dbData, DB_FAST_STAT)) != 0)
	traceEvent(TRACE_ERROR, "Error while dumping DB stats for Data [%s]", db_strerror(rc));
}

