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
#define vec_reset(vec, i)  (vec[(i)/8] &= ~(1<<((i)%8)))  // clear the ith bit
#define vec_test(vec, i)   (vec[(i)/8] & (1<<((i)%8)))    // test the ith bit

#define valueptr(handler, agglvl, frag_id, offset) \
    ((tsdb_value*)(&handler->tslice[agglvl].frag[frag_id]->data[offset * handler->entry_size]))

struct tsdb_frag {
    uint8_t is_set[vec_size(ENTRIES_PER_FRAG)]; // which indexes have values
    uint8_t data[]; // values (C99 flexible array member)
};

typedef struct {
    uint32_t time;
    int agglvl;
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

static int raw_db_open(tsdb_handler *handler, DB **dbp, const char *path,
    const char *dbname, uint8_t readonly)
{
    int ret;
    if ((ret = db_create(dbp, handler->dbenv, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error creating DB handler %s: %s",
	    dbname, db_strerror(ret));
	return -1;
    }

    int mode = (readonly ? 00444 : 00666);
    if ((ret = (*dbp)->open(*dbp, NULL, path, dbname,
	DB_BTREE, (readonly ? 0 : DB_CREATE), mode)) != 0)
    {
	traceEvent(TRACE_ERROR, "Error opening DB %s %s (mode=%o): %s",
	    path, dbname, mode, db_strerror(ret));
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

static int raw_db_get(const tsdb_handler *handler, DB* db,
    const void *key, uint32_t key_len, void **value, uint32_t *value_len)
{
    DBT key_data, data;

    static uint8_t *db_get_buf = 0;
    static uint16_t db_get_buf_len = 0;

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

int tsdb_open(const char *path, tsdb_handler *handler,
    uint16_t num_values_per_entry,
    uint32_t rrd_slot_time_duration,
    uint8_t readonly)
{
    int ret;
    const char *dbhome = "."; // XXX should be configurable

    memset(handler, 0, sizeof(tsdb_handler));
    handler->readonly = readonly;
    handler->num_values_per_entry = 1;

    if ((ret = db_env_create(&handler->dbenv, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error creating DB env: %s",
	    db_strerror(ret));
	return -1;
    }

    int mode = (readonly ? 00444 : 00777);
    if ((ret = handler->dbenv->open(handler->dbenv, dbhome, DB_CREATE | DB_INIT_MPOOL, mode)) != 0) {
	traceEvent(TRACE_ERROR, "Error opening DB env: %s",
	    db_strerror(ret));
	return -1;
    }

    if (raw_db_open(handler, &handler->dbMeta,  path, "meta",  readonly) != 0) return -1;
    if (raw_db_open(handler, &handler->dbKey,   path, "key",   readonly) != 0) return -1;
    if (raw_db_open(handler, &handler->dbData,  path, "data",  readonly) != 0) return -1;

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
    initcfg(handler, uint32_t, rrd_slot_time_duration, rrd_slot_time_duration);
    initcfg(handler, uint16_t, num_values_per_entry,   num_values_per_entry);
    initcfg(handler, uint16_t, num_agglvls,            1);

    handler->entry_size = handler->num_values_per_entry * sizeof(tsdb_value);

    memset(&handler->state_compress, 0, sizeof(handler->state_compress));
    memset(&handler->state_decompress, 0, sizeof(handler->state_decompress));

    handler->agg[0].func = TSDB_AGG_NONE;
    handler->agg[0].steps = 1;
    handler->agg[0].period = handler->rrd_slot_time_duration; // XXX

    if (handler->num_agglvls > 1) {
	void *value;
	uint32_t value_len;
	if (raw_db_get(handler, handler->dbMeta, "agg", strlen("agg"), &value, &value_len) == 0) {
	    if (value_len != handler->num_agglvls * sizeof(tsdb_agg)) {
		traceEvent(TRACE_ERROR, "corrupt aggregation config %d != %d",
		    value_len, handler->num_agglvls * sizeof(tsdb_agg));
		handler->readonly = 1;
		return -1;
	    }
	}
	memcpy(handler->agg, value, value_len);
    }

    // load key_info for every key
    int rc;
    DBC *cursor;
    if ((rc = handler->dbKey->cursor(handler->dbKey, NULL, &cursor, 0)) != 0) {
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
    // XXX TODO: if new agg is not on an agg period boundary, disallow it or
    // go back and calculate it from historic values

    if (handler->num_agglvls >= MAX_NUM_AGGLVLS) {
	traceEvent(TRACE_ERROR, "Too many aggregation levels");
	return -1;
    }

    int agglvl = handler->num_agglvls++;
    handler->agg[agglvl].func = func;
    handler->agg[agglvl].steps = steps;
    handler->agg[agglvl].period = handler->agg[0].period * steps;

    raw_db_set(handler, handler->dbMeta, "agg", strlen("agg"),
	handler->agg, sizeof(tsdb_agg) * handler->num_agglvls);
    raw_db_set(handler, handler->dbMeta, "num_agglvls", strlen("num_agglvls"),
	&handler->num_agglvls, sizeof(handler->num_agglvls));

    return(0);
}

/*************************************************************************/

// Writes tslice to db (unless db is readonly).
// Also cleans up in-memory tslice, even if db is readonly.
static void tsdb_flush_tslice(tsdb_handler *handler, int agglvl)
{
    traceEvent(TRACE_INFO, "Flush %u agglvl=%d", handler->tslice[agglvl].time, agglvl);
    uint32_t frag_size = fragsize(handler);
    fragkey_t dbkey;
    uint32_t buf_len = frag_size + QLZ_OVERHEAD;
    char *buf = NULL;


    // Write fragments to the DB
    dbkey.time = handler->tslice[agglvl].time;
    dbkey.agglvl = agglvl;
    for (int f = 0; f < handler->tslice[agglvl].num_frags; f++) {
	if (!handler->tslice[agglvl].frag[f]) continue;
	if (handler->readonly) {
	    // do nothing
	} else if (handler->tslice[agglvl].frag_changed[f]) {
	    if (!buf)
		buf = (char*)emalloc(buf_len, "compression buffer");
	    if (!buf) return;
	    size_t compressed_len = qlz_compress(handler->tslice[agglvl].frag[f],
		buf, frag_size, &handler->state_compress);
	    traceEvent(TRACE_INFO, "Frag %d compression: %u -> %u (%.1f %%)",
		f, frag_size, compressed_len, compressed_len*100.0/frag_size);
	    dbkey.frag_id = f;
	    raw_db_set(handler, handler->dbData, &dbkey, sizeof(dbkey), buf, compressed_len);
	} else {
	    traceEvent(TRACE_INFO, "Skipping frag %d (unchanged)", f);
	}

	handler->tslice[agglvl].frag_changed[f] = 0;
	free(handler->tslice[agglvl].frag[f]);
	handler->tslice[agglvl].frag[f] = 0;
    }

    if (!handler->readonly) {
	if (handler->agg[agglvl].last_flush < handler->tslice[agglvl].time)
	    handler->agg[agglvl].last_flush = handler->tslice[agglvl].time;
	raw_db_set(handler, handler->dbMeta, "agg", strlen("agg"),
	    handler->agg, sizeof(tsdb_agg) * handler->num_agglvls);
    }

    memset(&handler->tslice[agglvl], 0, sizeof(tsdb_tslice));
    free(buf);
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
    raw_db_set(handler, handler->dbKey, tkip->key, strlen(tkip->key), rki, rki_size);
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

// Update the timeranges associated with each key, according to the is_set
// vector.
static int update_key_info(tsdb_handler *handler, int next_is_far)
{
    tsdb_agg *agg0 = &handler->agg[0];
    tsdb_tslice *tslice = &handler->tslice[0];
    if (handler->readonly || tslice->time == 0) return 0;
    int is_last = tslice->time >= agg0->last_flush;
    traceEvent(TRACE_INFO,
	"update_key_info: t=%u, last_flush=%u, is_last=%d, next_is_far=%d",
	tslice->time, agg0->last_flush, is_last, next_is_far); // XXX

    for (uint32_t idx = 0; idx < handler->lowest_free_index; idx++) {
	uint32_t block = idx / INFOS_PER_BLOCK;
	uint32_t offset = idx % INFOS_PER_BLOCK;
	tsdb_key_info_t *tkip = &handler->key_info_block[block][offset];
	int value_is_set = tslice->frag[tkip->frag_id] &&
	    vec_test(tslice->frag[tkip->frag_id]->is_set, tkip->offset);

	int i = tkip->n_timeranges - 1;
	int changed = 0;

	if (next_is_far && i >= 0 && tkip->timeranges[i].end == 0) {
	    // next time > active time + 1 step (uncommon)
	    traceEvent(TRACE_INFO,
		"update_key_info: idx=%u, t=%u, [%u..%u] terminate",
		idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
	    tkip->timeranges[i].end = agg0->last_flush;
	    changed = 1;
	    traceEvent(TRACE_INFO,
		"             --> idx=%u, t=%u, [%u..%u] terminate",
		idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
	}

	if (value_is_set) {
	    if (i >= 0 && tkip->timeranges[i].end == 0 &&
		tslice->time >= tkip->timeranges[i].start)
	    {
		// active time is within or after current range
		if (tslice->time <= agg0->last_flush + agg0->period)
		{
		    // Common case: active time is within current (unterminated)
		    // range and is no more than 1 step past the last flush.
		    // assert(!changed);
		    continue; // no change, no write

		} else {
		    // active time is after current range, ie more than 1 step
		    // past last flush
		    traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] post-current",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		    tkip->timeranges[i].end = agg0->last_flush;
		    traceEvent(TRACE_INFO,
			"             --> idx=%u, t=%u, [%u..%u] post-current",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		    int n = ++tkip->n_timeranges;
		    i++;
		    timerange_t *newtr = emalloc(n * sizeof(timerange_t), "timeranges");
		    if (!newtr) return -1;
		    memcpy(&newtr[0], &tkip->timeranges[0], i * sizeof(timerange_t));
		    free(tkip->timeranges);
		    tkip->timeranges = newtr;
		    tkip->timeranges[i].start = tslice->time;
		    tkip->timeranges[i].end = 0;
		    traceEvent(TRACE_INFO,
			"             --> idx=%u, t=%u, [%u..%u] post-current",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		}

	    } else {
		// search for historic timerange
		while (i >= 0 && tkip->timeranges[i].start > tslice->time) i--;
		if (i >= 0 && tslice->time <= tkip->timeranges[i].end) {
		    // historic timerange matches
		    /* traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] historic",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX */
		    if (!changed) continue; // no change, no write
		} else if (i >= 0 && tslice->time == tkip->timeranges[i].end + agg0->period) {
		    // time abuts end of historic timerange; extend it
		    traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] abut end",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		    tkip->timeranges[i].end = tslice->time;
		    traceEvent(TRACE_INFO,
			"             --> idx=%u, t=%u, [%u..%u] abut end %d/%d",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end, i, tkip->n_timeranges); // XXX
		    if (i+1 < tkip->n_timeranges &&
			tkip->timeranges[i].end + agg0->period == tkip->timeranges[i+1].start)
		    {
			// extended range i now abuts range i+1; merge them
			int n = --tkip->n_timeranges;
			size_t size = n * sizeof(timerange_t);
			timerange_t *newtr = emalloc(size, "timeranges");
			if (!newtr) return -1;
			memcpy(&newtr[0], &tkip->timeranges[0], (i+1) * sizeof(timerange_t));
			memcpy(&newtr[i+1], &tkip->timeranges[i+2], (n-i-1) * sizeof(timerange_t));
			newtr[i].end = tkip->timeranges[i+1].end;
			free(tkip->timeranges);
			tkip->timeranges = newtr;
			traceEvent(TRACE_INFO,
			    "             --> idx=%u, t=%u, [%u..%u] merge %d/%d",
			    idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end, i, n);// XXX
		    }
		} else if (i+1 < tkip->n_timeranges &&
		    tslice->time + agg0->period == tkip->timeranges[i+1].start)
		{
		    // time abuts start of historic timerange; extend it
		    i++;
		    traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] abut start",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		    tkip->timeranges[i].start = tslice->time;
		    traceEvent(TRACE_INFO,
			"             --> idx=%u, t=%u, [%u..%u] abut start %d/%d",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end, i, tkip->n_timeranges); // XXX
		} else {
		    // no match found; create a new timerange
		    int n = ++tkip->n_timeranges;
		    i++;
		    timerange_t *newtr = emalloc(n * sizeof(timerange_t), "timeranges");
		    if (!newtr) return -1;
		    if (n > 1) {
			memcpy(&newtr[0], &tkip->timeranges[0], i * sizeof(timerange_t));
			memcpy(&newtr[i+1], &tkip->timeranges[i], (n-i-1) * sizeof(timerange_t));
			free(tkip->timeranges);
		    }
		    tkip->timeranges = newtr;
		    tkip->timeranges[i].start = tslice->time;
		    tkip->timeranges[i].end = (!is_last || next_is_far) ?
			tslice->time : 0;
		    traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] create %d/%d",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end, i, n); // XXX
		}
	    }

	} else { // value is not set
	    if (tkip->n_timeranges == 0) {
		/*traceEvent(TRACE_INFO,
		    "update_key_info: idx=%u, t=%u, unset, no timeranges",
		    idx, tslice->time); // XXX */
		if (!changed) continue; // no change, no write
	    } else if (tkip->timeranges[i].end == 0 &&
		tslice->time >= tkip->timeranges[i].start)
	    {
		// current timerange matches; terminate it
		traceEvent(TRACE_INFO,
		    "update_key_info: idx=%u, t=%u, [%u..%u] unset current",
		    idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		tkip->timeranges[i].end = agg0->last_flush;
		traceEvent(TRACE_INFO,
		    "             --> idx=%u, t=%u, [%u..%u] unset current",
		    idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
	    } else {
		// search for historic timerange (possible only if we allow
		// deleting historic values)
		while (i >= 0 && tkip->timeranges[i].start > tslice->time) i--;
		if (i < 0 || tslice->time > tkip->timeranges[i].end) { // no match found
		    if (!changed) continue; // no change, no write
		} else if (tkip->timeranges[i].start == tslice->time &&
		    tkip->timeranges[i].end == tslice->time)
		{
		    // timerange would become empty; delete it
		    traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] unset historic clear",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		    int n = --tkip->n_timeranges;
		    memmove(&tkip->timeranges[i], &tkip->timeranges[i+1],
			(n - i) * sizeof(timerange_t));
		} else if (tkip->timeranges[i].start == tslice->time) {
		    // delete first step of timerange
		    traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] unset historic start",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		    tkip->timeranges[i].start += agg0->period;
		    traceEvent(TRACE_INFO,
			"             --> idx=%u, t=%u, [%u..%u] unset historic start",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		} else if (tkip->timeranges[i].end == tslice->time) {
		    // delete last step of timerange
		    traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] unset historic end",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		    tkip->timeranges[i].end -= agg0->period;
		    traceEvent(TRACE_INFO,
			"             --> idx=%u, t=%u, [%u..%u] unset historic end",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		} else {
		    // split time range
		    traceEvent(TRACE_INFO,
			"update_key_info: idx=%u, t=%u, [%u..%u] unset historic split",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end); // XXX
		    int n = ++tkip->n_timeranges;
		    // Push every item after i forward one slot, and put a
		    // copy of item i in slot i+1.
		    timerange_t *newtr = emalloc(n * sizeof(timerange_t), "timeranges");
		    if (!newtr) return -1;
		    memcpy(&newtr[0], &tkip->timeranges[0], (i+1) * sizeof(timerange_t));
		    memcpy(&newtr[i+1], &tkip->timeranges[i], (n-i-1) * sizeof(timerange_t));
		    free(tkip->timeranges);
		    tkip->timeranges = newtr;
                    // update items i and i+1
		    tkip->timeranges[i].end = tslice->time - agg0->period;
		    tkip->timeranges[i+1].start = tslice->time + agg0->period;
		    traceEvent(TRACE_INFO,
			"             --> idx=%u, t=%u, [%u..%u] unset historic split %d/%d",
			idx, tslice->time, tkip->timeranges[i].start, tkip->timeranges[i].end, i, n); // XXX
		    traceEvent(TRACE_INFO,
			"             --> idx=%u, t=%u, [%u..%u] unset historic split %d/%d",
			idx, tslice->time, tkip->timeranges[i+1].start, tkip->timeranges[i+1].end, i+1, n); // XXX
		}
	    }
	}

	// write modified key info to db
	if (set_key_info(handler, tkip) != 0)
	    return -1;
    }
    return 0;
}

/*************************************************************************/

void tsdb_close(tsdb_handler *handler)
{
    if (!handler->is_open) {
	return;
    }

    if (!handler->readonly)
	traceEvent(TRACE_INFO, "Flushing database changes...");

    update_key_info(handler, 0);
    for (int agglvl = 0; agglvl < handler->num_agglvls; agglvl++) {
	tsdb_flush_tslice(handler, agglvl);
    }

    //handler->dbMeta->close(handler->dbMeta, 0);
    //handler->dbKey->close(handler->dbKey, 0);
    //handler->dbData->close(handler->dbData, 0);
    handler->dbenv->close(handler->dbenv, 0);
    handler->is_open = 0;
}

/*************************************************************************/

static const time_t time_base = 259200; // 00:00 on first sunday of 1970, UTC

uint32_t normalize_time(const tsdb_handler *handler, int agglvl, uint32_t *t)
{
    *t -= (*t - time_base) % handler->agg[agglvl].period;
    return *t;
}

/*************************************************************************/

static int load_frag(tsdb_handler *handler, uint32_t t, int agglvl,
    uint32_t frag_id)
{
    // load fragment
    fragkey_t dbkey;
    void *value;
    uint32_t value_len;
    dbkey.time = t;
    dbkey.agglvl = agglvl;
    dbkey.frag_id = frag_id;
    int rc = raw_db_get(handler, handler->dbData, &dbkey, sizeof(dbkey),
	&value, &value_len);
    traceEvent(TRACE_INFO, "load_frag t=%u, agglvl=%u, frag_id=%u: "
	"raw_db_get = %d", t, agglvl, frag_id, rc);
    if (rc != 0)
	return 0; // no match

    // decompress fragment
    size_t len = qlz_size_decompressed(value);
    // assert(len == fragsize(handler));
    void *ptr = malloc(len);
    if (!ptr) {
	traceEvent(TRACE_ERROR, "Can't allocate %u bytes for frag "
	    "t=%u, agglvl=%u, frag_id=%u", len, t, agglvl, frag_id);
	return -2; // error
    }
    len = qlz_decompress(value, ptr, &handler->state_decompress);
    traceEvent(TRACE_INFO, "decompressed frag t=%u, agglvl=%u, frag_id=%u: "
	"%u -> %u (%.1f%%)",
	t, agglvl, frag_id, value_len, len, len*100.0/value_len);

    handler->tslice[agglvl].frag[frag_id] = ptr;

    tsdb_tslice *tslice = &handler->tslice[agglvl];
    if (tslice->num_frags <= frag_id)
	tslice->num_frags = frag_id + 1;

    return 0;
}

int tsdb_goto_time(tsdb_handler *handler, uint32_t time_value, uint32_t flags)
{
    int rc;

    traceEvent(TRACE_INFO, "goto_time %u", time_value);

    for (int agglvl = 0; agglvl < handler->num_agglvls; agglvl++) {
	tsdb_tslice *tslice = &handler->tslice[agglvl];
	uint32_t t = time_value;

	normalize_time(handler, agglvl, &t);
	if (tslice->time == t) {
	    traceEvent(TRACE_INFO, "goto_time %u, agglvl=%d: already loaded", t, agglvl);
	    continue;
	}

	if (agglvl == 0) {
	    int next_is_far = t > tslice->time + handler->agg[0].period;
	    if (update_key_info(handler, next_is_far) != 0)
		return -1;
	}

	// Flush tslice if loaded
	tsdb_flush_tslice(handler, agglvl);

	tslice->load_on_demand = !!(flags & TSDB_LOAD_ON_DEMAND);
	tslice->time = t;

	if (tslice->load_on_demand)
	    continue;

	int loaded = 0;
	tslice->num_frags = (handler->lowest_free_index + ENTRIES_PER_FRAG - 1) /
	    ENTRIES_PER_FRAG;
	for (uint32_t frag_id = 0; frag_id < tslice->num_frags; frag_id++) {
	    if ((rc = load_frag(handler, t, agglvl, frag_id)) != 0)
		return rc;
	    if (tslice->frag[frag_id])
		loaded++;
	}
	traceEvent(TRACE_INFO, "goto_time %u: agglvl=%d, loaded %u/%u fragments",
	    time_value, agglvl, loaded, tslice->num_frags);

	tslice->growable = !!(flags & TSDB_GROWABLE);
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

    if (raw_db_get(handler, handler->dbKey, key, keylen, &ptr, &len) == 0) {
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

static int instantiate_frag(tsdb_handler *handler, int agglvl, uint32_t frag_id)
{
    tsdb_tslice *tslice = &handler->tslice[agglvl];

    if (frag_id > MAX_NUM_FRAGS) {
	traceEvent(TRACE_ERROR, "Internal error: frag_id %u > %u",
	    frag_id, MAX_NUM_FRAGS);
	return -2;
    }

    if (tslice->frag[frag_id]) {
	// We already have the right fragment.  Do nothing.

    } else if (tslice->load_on_demand || handler->readonly) {
	// We should load the fragment.
	// TODO: optionally, unload other fragments?
	int rc = load_frag(handler, tslice->time, agglvl, frag_id);
	if (rc != 0)
	    return rc;
	if (!tslice->frag[frag_id])
	    return 1; // fragment not found

    } else if (tslice->growable) {
	// We should allocate a new fragment.
	traceEvent(TRACE_NORMAL, "grow tslice for time %u, agglvl %d, frag_id %u",
	    tslice->time, agglvl, frag_id);
	tslice->frag[frag_id] = calloc(1, fragsize(handler));
	if (!tslice->frag[frag_id]) {
	    traceEvent(TRACE_WARNING, "Can't allocate %u bytes to grow tslice "
		"for time %u, agglvl %d, frag_id %u",
		fragsize(handler), tslice->time, agglvl, frag_id);
	    return -2;
	}
	tslice->num_frags = frag_id + 1;
	traceEvent(TRACE_INFO, "Grew tslice to %u elements",
	    tslice->num_frags * ENTRIES_PER_FRAG);

    } else {
	traceEvent(TRACE_ERROR, "Bad frag_id %u", frag_id);
	return -1;
    }

    return 0;
}

/*************************************************************************/

static inline uint32_t min(uint32_t a, uint32_t b) { return a < b ? a : b; }

static inline uint32_t max(uint32_t a, uint32_t b) { return a > b ? a : b; }

int tsdb_set(tsdb_handler *handler, tsdb_key_info_t *tkip, const tsdb_value *valuep)
{
    int rc;

    if (!handler->is_open || handler->readonly)
	return -1;
    if ((rc = instantiate_frag(handler, 0, tkip->frag_id)) != 0) {
	handler->readonly = 1;
	return rc;
    }

    traceEvent(TRACE_INFO, "tsdb_set %u %u", handler->tslice[0].time, valuep[0]); // XXX

    uint8_t was_set = vec_test(handler->tslice[0].frag[tkip->frag_id]->is_set, tkip->offset);
    tsdb_value *oldvaluep = valueptr(handler, 0, tkip->frag_id, tkip->offset);

    for (int agglvl = 1; agglvl < handler->num_agglvls; agglvl++) {
	if ((rc = instantiate_frag(handler, agglvl, tkip->frag_id)) != 0) {
	    handler->readonly = 1;
	    return rc;
	}

	// Aggregate *valuep into tslice[agglvl]
	uint8_t changed = 0;
	uint8_t failed = 0;
	tsdb_value *aggval = valueptr(handler, agglvl, tkip->frag_id, tkip->offset);

	uint32_t aggstart = handler->tslice[agglvl].time;
	uint32_t aggend = aggstart + (handler->agg[agglvl].steps - 1) * handler->agg[0].period;

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
	    agglvl, aggstart, aggend, aggval[0], n); // XXX

	switch (handler->agg[agglvl].func) {
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
		    uint32_t tr_end = tr[tri].end != 0 ?
			tr[tri].end : handler->agg[0].last_flush;
		    uint32_t overlap_end = min(aggend, tr_end);
		    if (tri < 0 || handler->tslice[0].time >= overlap_end) {
			// no timerange overlaps agg, or active time >= overlap_end
			aggval[i] = valuep[i];
			changed = 1;
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
	    vec_set(handler->tslice[agglvl].frag[tkip->frag_id]->is_set, tkip->offset);
	    handler->tslice[agglvl].frag_changed[tkip->frag_id] = 1;
	    traceEvent(TRACE_INFO, "Succesfully set value "
		"[agglvl=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u]",
		agglvl, tkip->frag_id, tkip->offset, handler->entry_size);
	} else if (failed) {
	    traceEvent(TRACE_ERROR, "Failed to set value "
		"[agglvl=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u]",
		agglvl, tkip->frag_id, tkip->offset, handler->entry_size);
	    handler->readonly = 1;
	    return -1;
	}
    }

    // Set value at agglvl 0 (after aggregations because aggregations need
    // both old and new values)
    memcpy(oldvaluep, valuep, handler->entry_size);
    vec_set(handler->tslice[0].frag[tkip->frag_id]->is_set, tkip->offset);
    handler->tslice[0].frag_changed[tkip->frag_id] = 1;
    traceEvent(TRACE_INFO, "Succesfully set value "
	"[agglvl=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u]",
	0, tkip->frag_id, tkip->offset, handler->entry_size);

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
    const tsdb_value **valuepp, int agglvl)
{
    int rc;
    tsdb_tslice *tslice = &handler->tslice[agglvl];

    if (!handler->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if ((rc = instantiate_frag(handler, agglvl, tkip->frag_id)) == 0) {
	if (!vec_test(tslice->frag[tkip->frag_id]->is_set, tkip->offset)) {
	    traceEvent(TRACE_WARNING, "Value unset (v): %u %d %s",
		tslice->time, agglvl, tkip->key);
	    *valuepp = NULL;
	    return 1;
	}
	*valuepp = valueptr(handler, agglvl, tkip->frag_id, tkip->offset);
	traceEvent(TRACE_INFO, "Succesfully read value [offset=%" PRIu32 "][value_len=%u]",
	    tkip->offset, handler->entry_size);

    } else {
	if (rc == 1)
	    traceEvent(TRACE_WARNING, "Value unset (f): %u %d %s",
		tslice->time, agglvl, tkip->key);
	*valuepp = NULL;
	return rc;
    }

    return 0;
}

int tsdb_get_by_key(tsdb_handler *handler, const char *key,
    const tsdb_value **valuepp, int agglvl)
{
    int rc;
    tsdb_key_info_t *tkip;

    if ((rc = tsdb_get_key_info(handler, key, &tkip, 0)) != 0) {
	return rc;
    }
    return tsdb_get(handler, tkip, valuepp, agglvl);
}

/*************************************************************************/

int tsdb_keywalk_start(tsdb_handler *handler)
{
    int rc;

    if ((rc = handler->dbKey->cursor(handler->dbKey, NULL, &handler->keywalk, 0)) != 0) {
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
    if ((rc = handler->dbKey->stat_print(handler->dbKey, DB_FAST_STAT)) != 0)
	traceEvent(TRACE_ERROR, "Error while dumping DB stats for Key [%s]", db_strerror(rc));
    if ((rc = handler->dbData->stat_print(handler->dbData, DB_FAST_STAT)) != 0)
	traceEvent(TRACE_ERROR, "Error while dumping DB stats for Data [%s]", db_strerror(rc));
}

