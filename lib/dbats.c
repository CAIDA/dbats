/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lessed General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 */

#include "tsdb_api.h"


/* *********************************************************************** */

static void map_raw_set(const tsdb_handler *handler,
    const char *key, u_int32_t key_len,
    const void *value, u_int32_t value_len);

static void map_raw_delete(const tsdb_handler *handler,
    const char *key, u_int32_t key_len);

static int map_raw_get(const tsdb_handler *handler,
    const char *key, u_int32_t key_len,
    void **value, u_int32_t *value_len);

/* *********************************************************************** */

int tsdb_open(const char *tsdb_path, tsdb_handler *handler,
    u_int16_t num_values_per_entry,
    u_int32_t rrd_slot_time_duration,
    u_int8_t read_only_mode)
{
    int ret, mode;

    memset(handler, 0, sizeof(tsdb_handler));
    handler->read_only_mode = read_only_mode;
    handler->num_values_per_entry = 1;

    /* DB */
    if ((ret = db_create(&handler->db, NULL, 0)) != 0) {
	traceEvent(TRACE_ERROR, "Error while creating DB handler [%s]", db_strerror(ret));
	return(-1);
    }

    mode = (read_only_mode ? 00444 : 00777 );
    if ((ret = handler->db->open(handler->db,
	NULL, (const char*)tsdb_path, NULL,
	DB_BTREE, (read_only_mode ? 0 : DB_CREATE), mode)) != 0)
    {
	traceEvent(TRACE_ERROR, "Error while opening DB %s [%s][r/o=%u,mode=%o]", 
	    tsdb_path, db_strerror(ret), read_only_mode, mode);
	return(-1);
    }

#define initcfg(handler, type, field, defaultval) \
    do { \
	void *value; \
	u_int32_t value_len; \
	if (map_raw_get(handler, #field, strlen(#field), &value, &value_len) == 0) { \
	    handler->field = *((type*)value); \
	} else if (!handler->read_only_mode) { \
	    handler->field = (defaultval); \
	    map_raw_set(handler, #field, strlen(#field), \
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
	if (map_raw_get(handler, "agg", strlen("agg"), &value, &value_len) == 0) {
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

    map_raw_set(handler, "agg", strlen("agg"), handler->agg, sizeof(tsdb_agg) * handler->num_agglvls);
    map_raw_set(handler, "num_agglvls", strlen("num_agglvls"), &handler->num_agglvls, sizeof(handler->num_agglvls));

    return(0);
}

/* *********************************************************************** */

static void map_raw_delete(const tsdb_handler *handler,
    const char *key, u_int32_t key_len)
{
    DBT key_data;

    if (handler->read_only_mode) {
	traceEvent(TRACE_WARNING, "Unable to delete value (read-only mode)");
	return;
    }

    memset(&key_data, 0, sizeof(key_data));
    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;

    if (handler->db->del(handler->db, NULL, &key_data, 0) != 0)
	traceEvent(TRACE_WARNING, "Error while deleting key");
}

/* *********************************************************************** */

static int map_raw_key_exists(const tsdb_handler *handler,
    const char *key, u_int32_t key_len)
{
    void *value;
    u_int value_len;

    return (map_raw_get(handler, key, key_len, &value, &value_len) == 0);
}

/* *********************************************************************** */

static void map_raw_set(const tsdb_handler *handler,
    const char *key, u_int32_t key_len,
    const void *value, u_int32_t value_len)
{
    DBT key_data, data;

    if (handler->read_only_mode) {
	traceEvent(TRACE_WARNING, "Unable to set value (read-only mode)");
	return;
    }

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));
    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    data.data = (void*)value; // assumption: DB won't write to *data.data
    data.size = value_len;

    if (handler->db->put(handler->db, NULL, &key_data, &data, 0) != 0)
	traceEvent(TRACE_WARNING, "Error while map_set(%.*s)", key_len, (char*)key);
}

/* *********************************************************************** */

static int map_raw_get(const tsdb_handler *handler, const char *key, u_int32_t key_len, void **value, u_int32_t *value_len)
{
    DBT key_data, data;

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));

    key_data.data = (void*)key; // assumption: DB won't write to *key_data.data
    key_data.size = key_len;
    if (handler->db->get(handler->db, NULL, &key_data, &data, 0) == 0) {
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

#define QLZ_OVERHEAD 400

static void tsdb_flush_tslice(tsdb_handler *handler, int agglvl)
{
    traceEvent(TRACE_INFO, "flush %u agglvl=%d", handler->tslice[agglvl].time, agglvl);
    u_int fragment_size = handler->entry_size * ENTRIES_PER_FRAG;
    char dbkey[32];
    u_int buf_len = fragment_size + QLZ_OVERHEAD;
    char *buf = (char*)malloc(buf_len);
    if (!buf) {
	traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", buf_len);
	return;
    }

    /* Write fragments to the DB */
    for (int f=0; f < handler->tslice[agglvl].num_fragments; f++) {
	if (!handler->tslice[agglvl].fragment[f]) continue;

	if (!handler->read_only_mode && handler->tslice[agglvl].fragment_changed[f]) {
	    u_int compressed_len = qlz_compress(handler->tslice[agglvl].fragment[f],
		buf, fragment_size, &handler->state_compress);
	    traceEvent(TRACE_INFO, "Compression %u -> %u [fragment %u] [%.1f %%]",
		fragment_size, compressed_len, f,
		compressed_len*100.0/fragment_size);
	    snprintf(dbkey, sizeof(dbkey), "%u-%d-%u", handler->tslice[agglvl].time, agglvl, f);
	    map_raw_set(handler, dbkey, strlen(dbkey), buf, compressed_len);
	} else {
	    traceEvent(TRACE_INFO, "Skipping agglvl %d fragment %d (unchanged)", agglvl, f);
	}

	handler->tslice[agglvl].fragment_changed[f] = 0;
	free(handler->tslice[agglvl].fragment[f]);
	handler->tslice[agglvl].fragment[f] = 0;
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

    if (!handler->read_only_mode)
	map_raw_set(handler, "lowest_free_index", strlen("lowest_free_index"),
	    &handler->lowest_free_index, sizeof(handler->lowest_free_index));

    for (int agglvl = 0; agglvl < handler->num_agglvls; agglvl++) {
	tsdb_flush_tslice(handler, agglvl);
    }

    if (!handler->read_only_mode)
	traceEvent(TRACE_INFO, "Flushing database changes...");

    handler->db->close(handler->db, 0);
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

static void reserve_key_index(const tsdb_handler *handler, u_int32_t idx)
{
    char dbkey[32];

    snprintf(dbkey, sizeof(dbkey), "rsv-%u", idx);
    map_raw_set(handler, dbkey, strlen(dbkey), "", 0);
}

/* *********************************************************************** */

static void unreserve_key_index(const tsdb_handler *handler, u_int32_t idx)
{
    char dbkey[32];

    snprintf(dbkey, sizeof(dbkey), "rsv-%u", idx);
    map_raw_delete(handler, dbkey, strlen(dbkey));
}

/* *********************************************************************** */

static int key_index_in_use(const tsdb_handler *handler, u_int32_t idx)
{
    char dbkey[32];

    snprintf(dbkey, sizeof(dbkey), "rsv-%u", idx);
    return(map_raw_key_exists(handler, dbkey, strlen(dbkey)));
}

/* *********************************************************************** */

static int get_key_index(const tsdb_handler *handler, int agglvl,
    const char *key, u_int32_t *key_idx)
{
    void *ptr;
    u_int32_t len;
    char dbkey[128] = { 0 };
    const tsdb_tslice *tslice = &handler->tslice[agglvl];

    snprintf(dbkey, sizeof(dbkey), "map-%s", key);

    if (map_raw_get(handler, dbkey, strlen(dbkey), &ptr, &len) == 0) {
	tsdb_key_mapping *mappings = (tsdb_key_mapping*)ptr;
	u_int i;
	u_int found = 0;
	u_int num_mappings = len / sizeof(tsdb_key_mapping);

	for (i=0; i<num_mappings; i++) {
	    if ((tslice->time >= mappings[i].time_start) &&
		((mappings[i].time_end == 0) ||
		(tslice->time < mappings[i].time_end)))
	    {
	        *key_idx = mappings[i].key_idx;
	        found = 1;
	        break;
	    }
	}

	//free(ptr);
	// traceEvent(TRACE_INFO, "[GET] Mapping %u -> %u", idx, *key_idx);
	return(found ? 0 : -1);
    }

    return(-1);
}

/* *********************************************************************** */

static int drop_key_index(const tsdb_handler *handler, const char *key,
    u_int32_t time_value, u_int32_t *key_idx)
{
    void *ptr;
    u_int32_t len;
    char dbkey[128];

    // XXX TODO: allowed only if time_value is the latest time ever seen

    snprintf(dbkey, sizeof(dbkey), "map-%s", key);

    if (map_raw_get(handler, dbkey, strlen(dbkey), &ptr, &len) == 0) {
	tsdb_key_mapping *mappings = (tsdb_key_mapping*)ptr;
	u_int i;
	u_int found = 0;
	u_int num_mappings = len / sizeof(tsdb_key_mapping);

	for (i=0; i<num_mappings; i++) {
	    if (mappings[i].time_end == 0) {
	        mappings[i].time_end = time_value;
	        found = 1;
	        map_raw_set(handler, dbkey, strlen(dbkey), &ptr, len);
	        break;
	    }
	}

	//free(ptr);
	// traceEvent(TRACE_INFO, "[GET] Mapping %u -> %u", key, *key_idx);
	return(found ? 0 : -1);
    }

    return(-1);
}

/* *********************************************************************** */

// XXX don't un-reserve mapping until the end of all agglvls that use it
void tsdb_drop_key(const tsdb_handler *handler,
    const char *key, u_int32_t time_value)
{
    u_int32_t key_idx = 0;

    if (drop_key_index(handler, key, time_value, &key_idx) == 0) {
	traceEvent(TRACE_INFO, "Key %s mapped to index %u", key, key_idx);
	unreserve_key_index(handler, key_idx);
    } else
	traceEvent(TRACE_WARNING, "Unable to drop key %s", key);
}

/* *********************************************************************** */

static void set_key_index(const tsdb_handler *handler, const char *key, u_int32_t key_idx)
{
    char dbkey[128];
    tsdb_key_mapping mapping;

    snprintf(dbkey, sizeof(dbkey), "map-%s", key);
    mapping.time_start = handler->tslice[0].time; // XXX
    mapping.time_end = 0;
    mapping.key_idx = key_idx;
    map_raw_set(handler, dbkey, strlen(dbkey), &mapping, sizeof(mapping));

    traceEvent(TRACE_INFO, "[SET] Mapping %s -> %u", key, key_idx);
}

/* *********************************************************************** */

int tsdb_goto_time(tsdb_handler *handler, u_int32_t time_value, uint32_t flags)
{
    int rc;
    void *value;
    u_int32_t value_len;
    char dbkey[32];

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

	u_int32_t fragment_id = 0;
	snprintf(dbkey, sizeof(dbkey), "%u-%d-%u", t, agglvl, fragment_id);
	rc = map_raw_get(handler, dbkey, strlen(dbkey), &value, &value_len);
	traceEvent(TRACE_INFO, "goto_time %u, agglvl=%d: map_raw_get -> %d", time_value, agglvl, rc);

	if (rc == -1) {
	    if (!(flags & TSDB_CREATE)) {
		traceEvent(TRACE_INFO, "Unable to goto time %u", time_value);
		return(-1);
	    }
	    traceEvent(TRACE_INFO, "new time %u", time_value);

	    /* Create an empty tslice */
	    tslice->num_fragments = 0;

	} else {
	    /* We need to decompress data */
	    traceEvent(TRACE_INFO, "loaded time %u", t);
	    uint32_t len;
	    fragment_id = 0;
	    tslice->num_fragments = 0;

	    while (1) {
		len = qlz_size_decompressed(value);

		uint8_t *newfrag = (u_int8_t*)malloc(len);
		if (!newfrag) {
		    traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", len);
		    free(value);
		    return(-2);
		}
		len = qlz_decompress(value, newfrag, &handler->state_decompress);
		traceEvent(TRACE_NORMAL, "Decompression %u -> %u [slice %d] [fragment %u] [%.1f %%]",
		    value_len, len, agglvl, fragment_id, len*100.0/value_len);

		tslice->fragment[fragment_id] = newfrag;
		fragment_id++;

		snprintf(dbkey, sizeof(dbkey), "%u-%d-%u", t, agglvl, fragment_id);
		if (map_raw_get(handler, dbkey, strlen(dbkey), &value, &value_len) == -1)
		    break; /* No more fragments */
	    } /* while */

	    tslice->num_fragments = fragment_id;

	    traceEvent(TRACE_INFO, "Moved to time %u", time_value);
	}

	tslice->growable = !!(flags & TSDB_GROWABLE);
    }
    return(0);
}

/* *********************************************************************** */

static int mapKeyToIndex(tsdb_handler *handler, int agglvl,
    const char *key, u_int32_t *key_idx, u_int8_t create_idx_if_needed)
{
    /* Check if this is a known key */
    if (get_key_index(handler, agglvl, key, key_idx) == 0) {
	traceEvent(TRACE_INFO, "Key %s mapped to index %u", key, *key_idx);
	return(0);
    }

    if (!create_idx_if_needed) {
	traceEvent(TRACE_INFO, "Unable to find key %s", key);
	return(-1);
    }

    while (handler->lowest_free_index < MAX_NUM_FRAGMENTS * ENTRIES_PER_FRAG) {
	*key_idx = handler->lowest_free_index++;
	if (!key_index_in_use(handler, *key_idx)) {
	    set_key_index(handler, key, *key_idx);
	    reserve_key_index(handler, *key_idx);
	    traceEvent(TRACE_INFO, "Key %s mapped to index %u", key, *key_idx);
	    return 0;
	    break;
	}
    }

    traceEvent(TRACE_ERROR, "Out of indexes");
    return -1;
}


/* *********************************************************************** */

// XXX TODO: if there's no match and create_idx_if_needed, and there is a
// match at a higher agglvl, use that (?)
static int getOffset(tsdb_handler *handler, int agglvl, const char *key,
    uint32_t *fragment_id, u_int64_t *offset, u_int8_t create_idx_if_needed)
{
    u_int32_t key_idx;

    if (mapKeyToIndex(handler, agglvl, key, &key_idx, create_idx_if_needed) == -1) {
	traceEvent(TRACE_INFO, "Unable to find key %s", key);
	return(-1);
    } else {
	traceEvent(TRACE_INFO, "%s mapped to idx %u", key, key_idx);
    }

    *fragment_id = key_idx / ENTRIES_PER_FRAG;
    *offset = (key_idx % ENTRIES_PER_FRAG) * handler->entry_size;
    tsdb_tslice *tslice = &handler->tslice[agglvl];

    if (*fragment_id > MAX_NUM_FRAGMENTS) {
	traceEvent(TRACE_ERROR, "Internal error [%u > %u]", *fragment_id, MAX_NUM_FRAGMENTS);
	return -2;
    }

    if (tslice->fragment[*fragment_id]) {
	// We already have the right fragment.  Do nothing.

    } else if (tslice->load_on_demand || handler->read_only_mode) {
	// We should load the fragment.
	u_int32_t value_len;
	char dbkey[32];
	void *value;

	// TODO: optionally, unload other fragments?

	snprintf(dbkey, sizeof(dbkey), "%u-%d-%u", tslice->time, agglvl, *fragment_id);
	if (map_raw_get(handler, dbkey, strlen(dbkey), &value, &value_len) == -1)
	    return(-1);

	uint32_t len = qlz_size_decompressed(value);

	tslice->fragment[*fragment_id] = (u_int8_t*)malloc(len);
	if (!tslice->fragment[*fragment_id]) {
	    traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", len);
	    return(-2);
	}

	qlz_decompress(value, tslice->fragment[*fragment_id], &handler->state_decompress);
	//free(value);

	if (tslice->num_fragments <= *fragment_id)
	    tslice->num_fragments = *fragment_id + 1;

    } else if (tslice->growable) {
	// We should allocate a new fragment.
	u_int32_t frag_len = ENTRIES_PER_FRAG * handler->entry_size;
	u_int8_t *ptr    = malloc(frag_len);

	if (ptr) {
	    memset(ptr, handler->default_unknown_value, frag_len);       
	    tslice->fragment[*fragment_id] = ptr;
	    tslice->num_fragments = *fragment_id + 1;

	    traceEvent(TRACE_INFO, "Grown table to %u elements",
		tslice->num_fragments * ENTRIES_PER_FRAG);

	} else {
	    traceEvent(TRACE_WARNING, "Not enough memory (%u bytes): unable to grow table", frag_len);
	    return(-2);
	}

    } else {
	traceEvent(TRACE_ERROR, "Index %u out of range %u...%u (%u)",
	    key_idx, 0,
	    tslice->num_fragments * ENTRIES_PER_FRAG - 1,
	    tslice->num_fragments);
	return(-1);
    }

    return(0);
}

/* *********************************************************************** */

int tsdb_set(tsdb_handler *handler, const char *key, const tsdb_value *valuep)
{
    u_int64_t offset;
    uint32_t fragment_id;

    if (!handler->is_open)
	return -1;

    if (getOffset(handler, 0, key, &fragment_id, &offset, 1) == 0) {
	tsdb_value *dest = (tsdb_value*)(handler->tslice[0].fragment[fragment_id] + offset);
	memcpy(dest, valuep, handler->entry_size);
	handler->tslice[0].fragment_changed[fragment_id] = 1;
	traceEvent(TRACE_INFO, "Succesfully set value "
	    "[agglvl=%d][frag_id=%u][offset=%" PRIu64 "][value_len=%u]",
	    0, fragment_id, offset, handler->entry_size);

	for (int agglvl = 1; agglvl < handler->num_agglvls; agglvl++) {
	    if (getOffset(handler, agglvl, key, &fragment_id, &offset, 1) == 0) {
		uint8_t changed = 0;
		// Aggregate *valuep into tslice[agglvl]
		tsdb_value *aggval = (tsdb_value*)(handler->tslice[agglvl].fragment[fragment_id] + offset);
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
		    handler->tslice[agglvl].fragment_changed[fragment_id] = 1;

		traceEvent(TRACE_INFO, "Succesfully set value "
		    "[agglvl=%d][frag_id=%u][offset=%" PRIu64 "][value_len=%u]",
		    agglvl, fragment_id, offset, handler->entry_size);

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
    u_int64_t offset;
    uint32_t fragment_id;

    if (!handler->is_open) {
	*valuepp = &handler->default_unknown_value;
	return -1;
    }

    if (getOffset(handler, agglvl, key, &fragment_id, &offset, 0) == 0) {
	*valuepp = (tsdb_value*)(handler->tslice[agglvl].fragment[fragment_id] + offset);

	traceEvent(TRACE_INFO, "Succesfully read value [offset=%" PRIu64 "][value_len=%u]",
	    offset, handler->entry_size);

    } else {
	traceEvent(TRACE_ERROR, "Missing time");
	*valuepp = &handler->default_unknown_value;
	return -2;
    }

    return 0;
}

/* *********************************************************************** */

void tsdb_stat_print(const tsdb_handler *handler) {
    int ret;
    
    if ((ret = handler->db->stat_print(handler->db, DB_FAST_STAT)) != 0) {
	traceEvent(TRACE_ERROR, "Error while dumping DB stats [%s]", db_strerror(ret));
    }
}
