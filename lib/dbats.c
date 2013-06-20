/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lessed General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 */

#include <db.h>
#include "tsdb_api.h"


/* *********************************************************************** */

// bit vector
#define vec_size(N)        (((N)+7)/8)                    // size for N bits
#define vec_set(vec, i)    (vec[(i)/8] |= (1<<((i)%8)))   // set the ith bit
#define vec_reset(vec, i)  (vec[(i)/8] &= ~(1<<((i)%8)))  // clear the ith bit
#define vec_test(vec, i)   (vec[(i)/8] & (1<<((i)%8)))    // test the ith bit

#define valueptr(handler, agglvl, frag_id, offset) \
    ((tsdb_value*)(&handler->tslice[agglvl].frag[frag_id]->data[offset * handler->entry_size]))

struct tsdb_frag {
    uint8_t is_set[vec_size(ENTRIES_PER_FRAG)];
    uint8_t data[]; // flexible array member
};

typedef struct {
    uint32_t time;
    int agglvl;
    uint32_t frag_id;
} fragkey_t;

static void map_raw_set(const tsdb_handler *handler, DB *db,
    const void *key, u_int32_t key_len,
    const void *value, u_int32_t value_len);

static int map_raw_get(const tsdb_handler *handler, DB *db,
    const void *key, u_int32_t key_len,
    void **value, u_int32_t *value_len);

#define fragsize(handler) (sizeof(tsdb_frag) + ENTRIES_PER_FRAG * (handler)->entry_size)

/* *********************************************************************** */

static int raw_db_open(tsdb_handler *handler, DB **dbp, const char *path,
    const char *dbname, u_int8_t readonly)
{
    int ret;
    if ((ret = db_create(dbp, handler->dbenv, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error creating DB handler %s: %s",
	    dbname, db_strerror(ret));
	return -1;
    }

    int mode = (readonly ? 00444 : 00777);
    if ((ret = (*dbp)->open(*dbp, NULL, path, dbname,
	DB_BTREE, (readonly ? 0 : DB_CREATE), mode)) != 0)
    {
	traceEvent(TRACE_ERROR, "Error opening DB %s %s (mode=%o): %s",
	    path, dbname, mode, db_strerror(ret));
	return -1;
    }
    return 0;
}

int tsdb_open(const char *path, tsdb_handler *handler,
    u_int16_t num_values_per_entry,
    u_int32_t rrd_slot_time_duration,
    u_int8_t readonly)
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
	u_int32_t value_len; \
	if (map_raw_get(handler, handler->dbMeta, #field, strlen(#field), &value, &value_len) == 0) { \
	    handler->field = *((type*)value); \
	} else if (!handler->readonly) { \
	    handler->field = (defaultval); \
	    map_raw_set(handler, handler->dbMeta, #field, strlen(#field), \
		&handler->field, sizeof(handler->field)); \
	} \
	traceEvent(TRACE_INFO, "%-.22s = %u", #field, handler->field); \
    } while (0)

    initcfg(handler, u_int32_t, lowest_free_index,      0);
    initcfg(handler, u_int32_t, rrd_slot_time_duration, rrd_slot_time_duration);
    initcfg(handler, u_int16_t, num_values_per_entry,   num_values_per_entry);
    initcfg(handler, u_int16_t, num_agglvls,            1);

    handler->entry_size = handler->num_values_per_entry * sizeof(tsdb_value);

    memset(&handler->state_compress, 0, sizeof(handler->state_compress));
    memset(&handler->state_decompress, 0, sizeof(handler->state_decompress));

    handler->agg[0].func = TSDB_AGG_NONE;
    handler->agg[0].steps = 1;
    handler->agg[0].period = handler->rrd_slot_time_duration; // XXX

    if (handler->num_agglvls > 1) {
	void *value;
	uint32_t value_len;
	if (map_raw_get(handler, handler->dbMeta, "agg", strlen("agg"), &value, &value_len) == 0) {
	    if (value_len != handler->num_agglvls * sizeof(tsdb_agg)) {
		traceEvent(TRACE_ERROR, "corrupt aggregation config %d != %d",
		    value_len, handler->num_agglvls * sizeof(tsdb_agg));
		return -1;
	    }
	}
	memcpy(handler->agg, value, value_len);
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

    map_raw_set(handler, handler->dbMeta, "agg", strlen("agg"),
	handler->agg, sizeof(tsdb_agg) * handler->num_agglvls);
    map_raw_set(handler, handler->dbMeta, "num_agglvls", strlen("num_agglvls"),
	&handler->num_agglvls, sizeof(handler->num_agglvls));

    return(0);
}

/* *********************************************************************** */

/*
static void map_raw_delete(const tsdb_handler *handler, DB *db,
    const void *key, u_int32_t key_len)
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

/* *********************************************************************** */

/*
static int map_raw_key_exists(const tsdb_handler *handler, DB *db,
    const void *key, u_int32_t key_len)
{
    void *value;
    u_int value_len;

    return (map_raw_get(handler, db, key, key_len, &value, &value_len) == 0);
}
*/

/* *********************************************************************** */

static void map_raw_set(const tsdb_handler *handler, DB *db,
    const void *key, u_int32_t key_len,
    const void *value, u_int32_t value_len)
{
    DBT key_data, data;
    int ret;

    if (handler->readonly) {
	traceEvent(TRACE_WARNING, "Unable to set value (read-only mode)");
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
	traceEvent(TRACE_WARNING, "Error in map_set(%.*s): %s", key_len, (char*)key, db_strerror(ret));
}

/* *********************************************************************** */

#define QLZ_OVERHEAD 400

static int map_raw_get(const tsdb_handler *handler, DB* db,
    const void *key, u_int32_t key_len, void **value, u_int32_t *value_len)
{
    DBT key_data, data;

    static uint8_t *db_get_buf = 0;
    static uint16_t db_get_buf_len = 0;

    if (db_get_buf_len < ENTRIES_PER_FRAG * handler->entry_size + QLZ_OVERHEAD) {
	db_get_buf_len = ENTRIES_PER_FRAG * handler->entry_size + QLZ_OVERHEAD;
	if (db_get_buf) free(db_get_buf);
	db_get_buf = malloc(db_get_buf_len);
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
	//traceEvent(TRACE_WARNING, "map_raw_get failed: key=\"%.*s\"\n", len, (char*)key);
	return(-1);
    }
}

/* *********************************************************************** */

// Writes tslice to db (unless db is readonly).
// Also cleans up in-memory tslice even if db is readonly.
static void tsdb_flush_tslice(tsdb_handler *handler, int agglvl)
{
    traceEvent(TRACE_INFO, "flush %u agglvl=%d", handler->tslice[agglvl].time, agglvl);
    u_int frag_size = fragsize(handler);
    fragkey_t dbkey;
    u_int buf_len = frag_size + QLZ_OVERHEAD;
    char *buf = NULL;

    /* Write fragments to the DB */
    dbkey.time = handler->tslice[agglvl].time;
    dbkey.agglvl = agglvl;
    for (int f=0; f < handler->tslice[agglvl].num_frags; f++) {
	if (!handler->tslice[agglvl].frag[f]) continue;
	if (!handler->readonly && handler->tslice[agglvl].frag_changed[f]) {
	    if (!buf)
		buf = (char*)malloc(buf_len);
	    if (!buf) {
		traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", buf_len);
		return;
	    }
	    u_int compressed_len = qlz_compress(handler->tslice[agglvl].frag[f],
		buf, frag_size, &handler->state_compress);
	    traceEvent(TRACE_INFO, "Compression %u -> %u [frag %u] [%.1f %%]",
		frag_size, compressed_len, f,
		compressed_len*100.0/frag_size);
	    dbkey.frag_id = f;
	    map_raw_set(handler, handler->dbData, &dbkey, sizeof(dbkey), buf, compressed_len);
	} else {
	    traceEvent(TRACE_INFO, "Skipping agglvl %d frag %d (unchanged)", agglvl, f);
	}

	handler->tslice[agglvl].frag_changed[f] = 0;
	free(handler->tslice[agglvl].frag[f]);
	handler->tslice[agglvl].frag[f] = 0;
    }

    memset(&handler->tslice[agglvl], 0, sizeof(tsdb_tslice));

    free(buf);
}

/* *********************************************************************** */

void tsdb_close(tsdb_handler *handler)
{
    if (!handler->is_open) {
	return;
    }

    if (!handler->readonly)
	traceEvent(TRACE_INFO, "Flushing database changes...");

    for (int agglvl = 0; agglvl < handler->num_agglvls; agglvl++) {
	tsdb_flush_tslice(handler, agglvl);
    }

    //handler->dbMeta->close(handler->dbMeta, 0);
    //handler->dbKey->close(handler->dbKey, 0);
    //handler->dbData->close(handler->dbData, 0);
    handler->dbenv->close(handler->dbenv, 0);
    handler->is_open = 0;
}

/* *********************************************************************** */

static const time_t time_base = 259200; // 00:00 on first sunday of 1970, UTC

u_int32_t normalize_time(const tsdb_handler *handler, int agglvl, u_int32_t *t)
{
    *t -= (*t - time_base) % handler->agg[agglvl].period;
    return *t;
}

/* *********************************************************************** */

static int get_key_index(const tsdb_handler *handler,
    const char *key, u_int32_t *key_idx)
{
    void *ptr;
    u_int32_t len;
    if (map_raw_get(handler, handler->dbKey, key, strlen(key), &ptr, &len) == 0) {
	memcpy(key_idx, ptr, sizeof(*key_idx));
	return 0;
    }
    return -1;
}

/* *********************************************************************** */

static void set_key_index(const tsdb_handler *handler, const char *key, u_int32_t key_idx)
{
    map_raw_set(handler, handler->dbKey, key, strlen(key), &key_idx, sizeof(key_idx));
    traceEvent(TRACE_INFO, "[SET] Mapping %s -> %u", key, key_idx);
}

/* *********************************************************************** */

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
    int rc = map_raw_get(handler, handler->dbData, &dbkey, sizeof(dbkey),
	&value, &value_len);
    traceEvent(TRACE_INFO, "load_frag t=%u, agglvl=%u, frag_id=%u: "
	"map_raw_get = %d", t, agglvl, frag_id, rc);
    if (rc != 0)
	return -1; // no match

    // decompress fragment
    uint32_t len = qlz_size_decompressed(value);
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

int tsdb_goto_time(tsdb_handler *handler, u_int32_t time_value, uint32_t flags)
{
    int rc;

    traceEvent(TRACE_INFO, "goto_time %u", time_value);

    for (int agglvl = 0; agglvl < handler->num_agglvls; agglvl++) {
	tsdb_tslice *tslice = &handler->tslice[agglvl];
	u_int32_t t = time_value;

	normalize_time(handler, agglvl, &t);
	if (tslice->time == t) {
	    traceEvent(TRACE_INFO, "goto_time %u, agglvl=%d: already loaded", t, agglvl);
	    continue;
	}

	/* Flush tslice if loaded */
	tsdb_flush_tslice(handler, agglvl);

	tslice->load_on_demand = !!(flags & TSDB_LOAD_ON_DEMAND);
	tslice->time = t;

	if (tslice->load_on_demand)
	    continue;

	// load first frag
	rc = load_frag(handler, t, agglvl, tslice->num_frags);

	// XXX FIXME: During writing, if a frag existed but contained no set
	// values, it would not have been flushed to db, and this code would
	// think it doesn't exist and won't load frags following it.
	if (rc == 0) {
	    // found a first frag; load remaining frags
	    while (load_frag(handler, t, agglvl, tslice->num_frags) == 0) {}
	    traceEvent(TRACE_INFO, "Moved to time %u", time_value);

	} else if (rc == -1) {
	    // no first frag; create a new tslice if allowed
	    if (!(flags & TSDB_CREATE)) {
		traceEvent(TRACE_INFO, "Unable to goto time %u", time_value);
		return -1;
	    }
	    traceEvent(TRACE_INFO, "empty tslice t=%u agglvl=%d", time_value, agglvl);
	    tslice->num_frags = 0;

	} else {
	    return rc;
	}

	tslice->growable = !!(flags & TSDB_GROWABLE);
    }
    return 0;
}

/* *********************************************************************** */

static int mapKeyToIndex(tsdb_handler *handler,
    const char *key, u_int32_t *key_idx, u_int8_t create_idx_if_needed)
{
    /* Check if this is a known key */
    if (get_key_index(handler, key, key_idx) == 0) {
	traceEvent(TRACE_INFO, "Key %s mapped to index %u", key, *key_idx);
	return(0);
    }

    if (!create_idx_if_needed) {
	traceEvent(TRACE_INFO, "Unable to find key %s", key);
	return(-1);
    }

    if (handler->lowest_free_index < MAX_NUM_FRAGS * ENTRIES_PER_FRAG) {
	*key_idx = handler->lowest_free_index++;
	set_key_index(handler, key, *key_idx);
	traceEvent(TRACE_INFO, "Key %s mapped to index %u", key, *key_idx);
	map_raw_set(handler, handler->dbMeta,
	    "lowest_free_index", strlen("lowest_free_index"),
	    &handler->lowest_free_index, sizeof(handler->lowest_free_index));
	return 0;
    }

    traceEvent(TRACE_ERROR, "Out of indexes");
    return -1;
}


/* *********************************************************************** */

static int getOffset(tsdb_handler *handler, int agglvl, const char *key,
    uint32_t *frag_id, uint32_t *offset, u_int8_t create_idx_if_needed)
{
    u_int32_t key_idx;

    if (mapKeyToIndex(handler, key, &key_idx, create_idx_if_needed) == -1) {
	traceEvent(TRACE_INFO, "Unable to find key %s", key);
	return(-1);
    } else {
	traceEvent(TRACE_INFO, "%s mapped to idx %u", key, key_idx);
    }

    *frag_id = key_idx / ENTRIES_PER_FRAG;
    *offset = key_idx % ENTRIES_PER_FRAG;
    tsdb_tslice *tslice = &handler->tslice[agglvl];

    if (*frag_id > MAX_NUM_FRAGS) {
	traceEvent(TRACE_ERROR, "Internal error [%u > %u]", *frag_id, MAX_NUM_FRAGS);
	return -2;
    }

    if (tslice->frag[*frag_id]) {
	// We already have the right fragment.  Do nothing.

    } else if (tslice->load_on_demand || handler->readonly) {
	// We should load the fragment.
	// TODO: optionally, unload other fragments?
	int rc = load_frag(handler, tslice->time, agglvl, *frag_id);
	if (rc != 0)
	    return rc;

    } else if (tslice->growable) {
	// We should allocate a new fragment.
	traceEvent(TRACE_NORMAL, "grow table for time %u, agglvl %d, frag_id %u",
	    tslice->time, agglvl, *frag_id);
	tslice->frag[*frag_id] = calloc(1, fragsize(handler));
	if (!tslice->frag[*frag_id]) {
	    traceEvent(TRACE_WARNING, "Not enough memory to grow table for "
		"time %u, agglvl %d, frag_id %u", tslice->time, agglvl, *frag_id);
	    return -2;
	}
	tslice->num_frags = *frag_id + 1;
	traceEvent(TRACE_INFO, "Grew table to %u elements",
	    tslice->num_frags * ENTRIES_PER_FRAG);

    } else {
	traceEvent(TRACE_ERROR, "Index %u out of range %u...%u (%u)",
	    key_idx, 0,
	    tslice->num_frags * ENTRIES_PER_FRAG - 1,
	    tslice->num_frags);
	return(-1);
    }

    return(0);
}

/* *********************************************************************** */

int tsdb_set(tsdb_handler *handler, const char *key, const tsdb_value *valuep)
{
    uint32_t offset;
    uint32_t frag_id;

    if (!handler->is_open)
	return -1;

    if (getOffset(handler, 0, key, &frag_id, &offset, 1) == 0) {
	tsdb_value *dest = valueptr(handler, 0, frag_id, offset);
	memcpy(dest, valuep, handler->entry_size);
	vec_set(handler->tslice[0].frag[frag_id]->is_set, offset);
	handler->tslice[0].frag_changed[frag_id] = 1;
	traceEvent(TRACE_INFO, "Succesfully set value "
	    "[agglvl=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u]",
	    0, frag_id, offset, handler->entry_size);

	for (int agglvl = 1; agglvl < handler->num_agglvls; agglvl++) {
	    if (getOffset(handler, agglvl, key, &frag_id, &offset, 1) == 0) {
		uint8_t changed = 0;
		// Aggregate *valuep into tslice[agglvl]
		tsdb_value *aggval = valueptr(handler, agglvl, frag_id, offset);
		// n: number of steps contributing to aggregate (XXX temp hack)
		int n = (handler->tslice[0].time - handler->tslice[agglvl].time) / handler->agg[0].period + 1;
		switch (handler->agg[agglvl].func) {
		case TSDB_AGG_MIN:
		    for (int i = 0; i < handler->num_values_per_entry; i++) {
			if (n == 1 || valuep[i] < aggval[i]) {
			    aggval[i] = valuep[i];
			    changed = 1;
			}
		    }
		    break;
		case TSDB_AGG_MAX:
		    for (int i = 0; i < handler->num_values_per_entry; i++) {
			if (n == 1 || valuep[i] > aggval[i]) {
			    aggval[i] = valuep[i];
			    changed = 1;
			}
		    }
		    break;
		case TSDB_AGG_AVG:
		    for (int i = 0; i < handler->num_values_per_entry; i++) {
			if (n == 1) {
			    aggval[i] = valuep[i];
			    changed = 1;
			} else if (aggval[i] != valuep[i]) {
			    aggval[i] += ((/*signed*/double)valuep[i] - aggval[i]) / n + 0.5;
			    changed = 1;
			}
		    }
		    break;
		case TSDB_AGG_LAST:
		    for (int i = 0; i < handler->num_values_per_entry; i++) {
			if (aggval[i] != valuep[i]) {
			    aggval[i] = valuep[i];
			    changed = 1;
			}
		    }
		    break;
		case TSDB_AGG_SUM:
		    for (int i = 0; i < handler->num_values_per_entry; i++) {
			traceEvent(TRACE_INFO, "i=%d n=%d aggval=%" PRIu32 " val=%" PRIu32, i, n, aggval[i], valuep[i]);
			if (n == 1)
			    aggval[i] = valuep[i];
			else
			    aggval[i] += valuep[i];
		    }
		    changed = 1;
		    break;
		}

		if (changed)
		    handler->tslice[agglvl].frag_changed[frag_id] = 1;

		traceEvent(TRACE_INFO, "Succesfully set value "
		    "[agglvl=%d][frag_id=%u][offset=%" PRIu32 "][value_len=%u]",
		    agglvl, frag_id, offset, handler->entry_size);

	    } else {
		traceEvent(TRACE_ERROR, "Missing time");
		return -2;
	    }
	}

    } else {
	traceEvent(TRACE_ERROR, "Missing time");
	return -2;
    }

    return 0;
}

/* *********************************************************************** */

int tsdb_get(tsdb_handler *handler, const char *key, const tsdb_value **valuepp,
    int agglvl)
{
    uint32_t offset;
    uint32_t frag_id;

    if (!handler->is_open) {
	*valuepp = NULL;
	return -1;
    }

    if (getOffset(handler, agglvl, key, &frag_id, &offset, 0) == 0) {
	*valuepp = valueptr(handler, agglvl, frag_id, offset);

	traceEvent(TRACE_INFO, "Succesfully read value [offset=%" PRIu32 "][value_len=%u]",
	    offset, handler->entry_size);

    } else {
	traceEvent(TRACE_ERROR, "Missing time");
	*valuepp = NULL;
	return -2;
    }

    return 0;
}

int tsdb_keywalk_start(tsdb_handler *handler)
{
    int ret;

    if ((ret = handler->dbKey->cursor(handler->dbKey, NULL, &handler->keywalk, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error in tsdb_keywalk_start: %s", db_strerror(ret));
	return -1;
    }
    return 0;
}

int tsdb_keywalk_next(tsdb_handler *handler, char **key, int *len)
{
    int ret;
    DBT dbkey, dbdata;

    memset(&dbkey, 0, sizeof(dbkey));
    memset(&dbdata, 0, sizeof(dbdata));
    if ((ret = handler->keywalk->get(handler->keywalk, &dbkey, &dbdata, DB_NEXT)) == 0) {
	*key = dbkey.data;
	*len = dbkey.size;
	return 0;
    } else {
	if (ret != DB_NOTFOUND)
	    traceEvent(TRACE_ERROR, "Error in tsdb_keywalk_next: %s", db_strerror(ret));
	*key = 0;
	*len = 0;
	return -1;
    }
}

int tsdb_keywalk_end(tsdb_handler *handler)
{
    int ret;

    if ((ret = handler->keywalk->close(handler->keywalk)) != 0) {
	traceEvent(TRACE_ERROR, "Error in tsdb_keywalk_end: %s", db_strerror(ret));
	return -1;
    }
    return 0;
}

/* *********************************************************************** */

void tsdb_stat_print(const tsdb_handler *handler) {
    int ret;
    
    if ((ret = handler->dbMeta->stat_print(handler->dbMeta, DB_FAST_STAT)) != 0)
	traceEvent(TRACE_ERROR, "Error while dumping DB stats for Meta [%s]", db_strerror(ret));
    if ((ret = handler->dbKey->stat_print(handler->dbKey, DB_FAST_STAT)) != 0)
	traceEvent(TRACE_ERROR, "Error while dumping DB stats for Key [%s]", db_strerror(ret));
    if ((ret = handler->dbData->stat_print(handler->dbData, DB_FAST_STAT)) != 0)
	traceEvent(TRACE_ERROR, "Error while dumping DB stats for Data [%s]", db_strerror(ret));
}

