/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lessed General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 */

#include "tsdb_api.h"


/* *********************************************************************** */

static void map_raw_set(tsdb_handler *handler,
			void *key, u_int32_t key_len,
			void *value, u_int32_t value_len);

static void map_raw_delete(tsdb_handler *handler,
			   void *key, u_int32_t key_len);

static int map_raw_get(tsdb_handler *handler,
		       void *key, u_int32_t key_len,
		       void **value, u_int32_t *value_len);

/* *********************************************************************** */

int tsdb_open(char *tsdb_path, tsdb_handler *handler,
	      u_int16_t *num_values_per_entry,
	      u_int32_t rrd_slot_time_duration,
	      u_int8_t read_only_mode) {
  void *value;
  u_int32_t value_len;
  int ret, mode;

  memset(handler, 0, sizeof(tsdb_handler));
  handler->read_only_mode = read_only_mode;

  /* DB */
  if((ret = db_create(&handler->db, NULL, 0)) != 0) {
    traceEvent(TRACE_ERROR, "Error while creating DB handler [%s]", db_strerror(ret));
    return(-1);
  }

  mode = (read_only_mode ? 00444 : 00777 );
  if((ret = handler->db->open(handler->db,
			      NULL,
			      (const char*)tsdb_path,
			      NULL,
			      DB_BTREE,
			      (read_only_mode ? 0 : DB_CREATE),
			      mode)) != 0) {
    traceEvent(TRACE_ERROR, "Error while opening DB %s [%s][r/o=%u,mode=%o]", 
	       tsdb_path, db_strerror(ret), read_only_mode, mode);
    return(-1);
  }

  if(map_raw_get(handler, "lowest_free_index",
		 strlen("lowest_free_index"), &value, &value_len) == 0) {
    handler->lowest_free_index = *((u_int32_t*)value);
  } else {
    if(!handler->read_only_mode) {
      handler->lowest_free_index = 0;
      map_raw_set(handler, "lowest_free_index", strlen("lowest_free_index"),
		  &handler->lowest_free_index, sizeof(handler->lowest_free_index));
    }
  }

  if(map_raw_get(handler, "rrd_slot_time_duration",
		 strlen("rrd_slot_time_duration"), &value, &value_len) == 0) {
    handler->rrd_slot_time_duration = *((u_int32_t*)value);
  } else {
    if(!handler->read_only_mode) {
      handler->rrd_slot_time_duration = rrd_slot_time_duration;
      map_raw_set(handler, "rrd_slot_time_duration", strlen("rrd_slot_time_duration"),
		  &handler->rrd_slot_time_duration, sizeof(handler->rrd_slot_time_duration));
    }
  }

  if(map_raw_get(handler, "num_values_per_entry",
		 strlen("num_values_per_entry"), &value, &value_len) == 0) {
    *num_values_per_entry = handler->num_values_per_entry = *((u_int16_t*)value);
  } else {
    if(!handler->read_only_mode) {
      handler->num_values_per_entry = *num_values_per_entry;
      map_raw_set(handler, "num_values_per_entry", strlen("num_values_per_entry"),
		  &handler->num_values_per_entry, sizeof(handler->num_values_per_entry));
    }
  }

  handler->values_len = handler->num_values_per_entry * sizeof(tsdb_value);

  traceEvent(TRACE_INFO, "lowest_free_index:      %u", handler->lowest_free_index);
  traceEvent(TRACE_INFO, "rrd_slot_time_duration: %u", handler->rrd_slot_time_duration);
  traceEvent(TRACE_INFO, "num_values_per_entry:   %u", handler->num_values_per_entry);

  memset(&handler->state_compress, 0, sizeof(handler->state_compress));
  memset(&handler->state_decompress, 0, sizeof(handler->state_decompress));

  handler->is_open = 1;

  return(0);
}

/* *********************************************************************** */

static void map_raw_delete(tsdb_handler *handler,
			   void *key, u_int32_t key_len) {
  DBT key_data;

  if(handler->read_only_mode) {
    traceEvent(TRACE_WARNING, "Unable to delete value (read-only mode)");
    return;
  }

  memset(&key_data, 0, sizeof(key_data));
  key_data.data = key, key_data.size = key_len;

  if(handler->db->del(handler->db, NULL, &key_data, 0) != 0)
    traceEvent(TRACE_WARNING, "Error while deleting key");
}

/* *********************************************************************** */

static int map_raw_key_exists(tsdb_handler *handler,
			      void *key, u_int32_t key_len) {
  void *value;
  u_int value_len;

  if(map_raw_get(handler, key, key_len, &value, &value_len) == 0) {
    return(1);
  } else
    return(0);
}

/* *********************************************************************** */

static void map_raw_set(tsdb_handler *handler,
			void *key, u_int32_t key_len,
			void *value, u_int32_t value_len) {
  DBT key_data, data;

  if(handler->read_only_mode) {
    traceEvent(TRACE_WARNING, "Unable to set value (read-only mode)");
    return;
  }

  memset(&key_data, 0, sizeof(key_data));
  memset(&data, 0, sizeof(data));
  key_data.data = key, key_data.size = key_len;
  data.data = value, data.size = value_len;

  if(handler->db->put(handler->db, NULL, &key_data, &data, 0) != 0)
    traceEvent(TRACE_WARNING, "Error while map_set(%.*s)", key_len, (char*)key);
}

/* *********************************************************************** */

static int map_raw_get(tsdb_handler *handler,
		       void *key, u_int32_t key_len,
		       void **value, u_int32_t *value_len) {
  DBT key_data, data;

  memset(&key_data, 0, sizeof(key_data));
  memset(&data, 0, sizeof(data));

  key_data.data = key, key_data.size = key_len;
  if(handler->db->get(handler->db, NULL, &key_data, &data, 0) == 0) {
    *value = data.data, *value_len = data.size;
    return(0);
  } else {
    //int len = key_len;
    //traceEvent(TRACE_WARNING, "map_raw_get failed: key=\"%.*s\"\n", len, (char*)key);
    return(-1);
  }
}

/* *********************************************************************** */

static void tsdb_flush_chunk(tsdb_handler *handler) {
  char *compressed;
  u_int compressed_len, buf_len;
  u_int fragment_size = handler->values_len * CHUNK_GROWTH;
  char str[32];

  if (!handler->chunk.num_fragments) return;

  buf_len = fragment_size + 400 /* Static value */;
  compressed = (char*)malloc(buf_len);
  if(!compressed) {
    traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", buf_len);
    return;
  }

  /* Write fragments to the DB */
  for (int i=0; i < handler->chunk.num_fragments; i++) {

    if (!handler->chunk.fragment[i]) continue;

    if (!handler->read_only_mode && handler->chunk.fragment_changed[i]) {

      compressed_len = qlz_compress(handler->chunk.fragment[i],
				    compressed, fragment_size,
				    &handler->state_compress);

      traceEvent(TRACE_INFO, "Compression %u -> %u [fragment %u] [%.1f %%]",
		 fragment_size, compressed_len, i,
		 compressed_len*100.0/fragment_size);

      snprintf(str, sizeof(str), "%u-%u", handler->chunk.time, i);

      map_raw_set(handler, str, strlen(str), compressed, compressed_len);
    } else {
      traceEvent(TRACE_INFO, "Skipping fragment %u (unchanged)", i);
    }
    handler->chunk.fragment_changed[i] = 0;
    free(handler->chunk.fragment[i]);
    handler->chunk.fragment[i] = 0;
  }

  free(compressed);
  memset(&handler->chunk, 0, sizeof(handler->chunk));
}

/* *********************************************************************** */

void tsdb_close(tsdb_handler *handler) {

  if(!handler->is_open) {
    return;
  }

  if(!handler->read_only_mode)
    map_raw_set(handler, "lowest_free_index", strlen("lowest_free_index"),
		&handler->lowest_free_index, sizeof(handler->lowest_free_index));

  tsdb_flush_chunk(handler);

  if(!handler->read_only_mode)
    traceEvent(TRACE_INFO, "Flushing database changes...");

  handler->db->close(handler->db, 0);
}

/* *********************************************************************** */

static const time_t time_base = 259200; // 00:00 on first sunday of 1970, UTC

u_int32_t normalize_time(tsdb_handler *handler, u_int32_t *t) {
  *t -= (*t - time_base) % handler->rrd_slot_time_duration;
  return *t;
}

/* *********************************************************************** */

static void reserve_key_index(tsdb_handler *handler, u_int32_t idx) {
  char str[32];

  snprintf(str, sizeof(str), "rsv-%u", idx);
  map_raw_set(handler, str, strlen(str), "", 0);
}

/* *********************************************************************** */

static void unreserve_key_index(tsdb_handler *handler, u_int32_t idx) {
  char str[32];

  snprintf(str, sizeof(str), "rsv-%u", idx);
  map_raw_delete(handler, str, strlen(str));
}

/* *********************************************************************** */

static int key_index_in_use(tsdb_handler *handler, u_int32_t idx) {
  char str[32];

  snprintf(str, sizeof(str), "rsv-%u", idx);
  return(map_raw_key_exists(handler, str, strlen(str)));
}

/* *********************************************************************** */

static int get_key_index(tsdb_handler *handler, char *key, u_int32_t *value) {
  void *ptr;
  u_int32_t len;
  char str[128] = { 0 };

  snprintf(str, sizeof(str), "map-%s", key);

  if(map_raw_get(handler, str, strlen(str), &ptr, &len) == 0) {
    tsdb_key_mapping *mappings = (tsdb_key_mapping*)ptr;
    u_int i, found = 0, num_mappings = len / sizeof(tsdb_key_mapping);

    for(i=0; i<num_mappings; i++) {
      if((mappings[i].time_start <= handler->chunk.time)
	 && ((mappings[i].time_end == 0)
	     || (mappings[i].time_end <= handler->chunk.time /*???*/))) {
	*value = mappings[i].key_idx;
	found = 1;
	break;
      }
    } /* for */

    //free(ptr);
    // traceEvent(TRACE_INFO, "[GET] Mapping %u -> %u", idx, *value);
    return(found ? 0 : -1);
  }

  return(-1);
}

/* *********************************************************************** */

static int drop_key_index(tsdb_handler *handler, char *key,
			       u_int32_t time_value, u_int32_t *value) {
  void *ptr;
  u_int32_t len;
  char str[128];

  snprintf(str, sizeof(str), "map-%s", key);

  if(map_raw_get(handler, str, strlen(str), &ptr, &len) == 0) {
    tsdb_key_mapping *mappings = (tsdb_key_mapping*)ptr;
    u_int i, found = 0, num_mappings = len / sizeof(tsdb_key_mapping);

    for(i=0; i<num_mappings; i++) {
      if(mappings[i].time_end == 0) {
	mappings[i].time_end = time_value;
	found = 1;
	map_raw_set(handler, str, strlen(str), &ptr, len);
	break;
      }
    }

    //free(ptr);
    // traceEvent(TRACE_INFO, "[GET] Mapping %u -> %u", key, *value);
    return(found ? 0 : -1);
  }

  return(-1);
}

/* *********************************************************************** */

void tsdb_drop_key(tsdb_handler *handler,
		    char *key,
		    u_int32_t time_value) {
  u_int32_t key_idx = 0;

  if(drop_key_index(handler, key, time_value, &key_idx) == 0) {
    traceEvent(TRACE_INFO, "Key %s mapped to index %u", key, key_idx);
    unreserve_key_index(handler, key_idx);
  } else
    traceEvent(TRACE_WARNING, "Unable to drop key %s", key);
}

/* *********************************************************************** */

static void set_key_index(tsdb_handler *handler, char *key, u_int32_t value) {
  char str[128];
  tsdb_key_mapping mapping;

  snprintf(str, sizeof(str), "map-%s", key);
  mapping.time_start = handler->chunk.time; /* Courtesy of Francesco Fusco <fusco@ntop.org> */
  mapping.time_end = 0;
  mapping.key_idx = value;
  map_raw_set(handler, str, strlen(str), &mapping, sizeof(mapping));

  traceEvent(TRACE_INFO, "[SET] Mapping %s -> %u", key, value);
}

/* *********************************************************************** */

int tsdb_goto_time(tsdb_handler *handler,
		   u_int32_t time_value,
		   u_int8_t create_if_needed,
		   u_int8_t growable,
		   u_int8_t load_on_demand) {
  int rc;
  void *value;
  u_int32_t value_len, fragment_id = 0;
  char str[32];

  traceEvent(TRACE_INFO, "goto_time %u", time_value);

  normalize_time(handler, &time_value);
  if (handler->chunk.time == time_value) {
    traceEvent(TRACE_INFO, "goto_time %u: already loaded", time_value);
    return 0;
  }

  /* Flush chunk if loaded */
  tsdb_flush_chunk(handler);

  handler->chunk.load_on_demand = load_on_demand;
  handler->chunk.time = time_value;

  if (handler->chunk.load_on_demand)
    return(0);

  snprintf(str, sizeof(str), "%u-%u", time_value, fragment_id);
  rc = map_raw_get(handler, str, strlen(str), &value, &value_len);
  traceEvent(TRACE_INFO, "goto_time %u: map_raw_get -> %d", time_value, rc);

  if(rc == -1) {
    if(!create_if_needed) {
      traceEvent(TRACE_INFO, "Unable to goto time %u", time_value);
      return(-1);
    }
    traceEvent(TRACE_INFO, "new time %u", time_value);

    /* Create an empty chunk */
    handler->chunk.num_fragments = 0;

  } else {
    /* We need to decompress data */
    traceEvent(TRACE_INFO, "loaded time %u", time_value);
    uint32_t len;

    fragment_id = 0;
    handler->chunk.num_fragments = 0;

    while (1) {
      len = qlz_size_decompressed(value);

      uint8_t *newfrag = (u_int8_t*)malloc(len);
      if (!newfrag) {
	traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", len);
	free(value);
	return(-2);
      }
      len = qlz_decompress(value, newfrag, &handler->state_decompress);
      traceEvent(TRACE_NORMAL, "Decompression %u -> %u [fragment %u] [%.1f %%]",
		 value_len, len, fragment_id,
		 len*100.0/value_len);

      handler->chunk.fragment[fragment_id] = newfrag;
      fragment_id++;

      snprintf(str, sizeof(str), "%u-%u", time_value, fragment_id);
      if(map_raw_get(handler, str, strlen(str), &value, &value_len) == -1)
	break; /* No more fragments */
    } /* while */

    handler->chunk.num_fragments = fragment_id;

    traceEvent(TRACE_INFO, "Moved to time %u", time_value);
  }

  handler->chunk.growable = growable;

  return(0);
}

/* *********************************************************************** */

static int mapKeyToIndex(tsdb_handler *handler, char *key,
			  u_int32_t *value, u_int8_t create_idx_if_needed) {
  /* Check if this is a known value */
  if(get_key_index(handler, key, value) == 0) {
    traceEvent(TRACE_INFO, "Key %s mapped to index %u", key, *value);
    return(0);
  }

  if(!create_idx_if_needed) {
    traceEvent(TRACE_INFO, "Unable to find key %s", key);
    return(-1);
  }


  while (handler->lowest_free_index < MAX_NUM_FRAGMENTS * CHUNK_GROWTH) {
    *value = handler->lowest_free_index++;

    if(!key_index_in_use(handler, *value)) {
      set_key_index(handler, key, *value);
      reserve_key_index(handler, *value);
      traceEvent(TRACE_INFO, "Key %s mapped to index %u", key, *value);
      return 0;
      break;
    }
  }

  traceEvent(TRACE_ERROR, "Out of indexes");
  return -1;
}


/* *********************************************************************** */

static int getOffset(tsdb_handler *handler, char *key,
		     uint32_t *fragment_id, u_int64_t *offset, u_int8_t create_idx_if_needed) {
  u_int32_t key_index;

  if(mapKeyToIndex(handler, key, &key_index, create_idx_if_needed) == -1) {
    traceEvent(TRACE_INFO, "Unable to find key %s", key);
    return(-1);
  } else
    traceEvent(TRACE_INFO, "%s mapped to idx %u", key, key_index);

  *fragment_id = key_index / CHUNK_GROWTH;
  *offset = (key_index % CHUNK_GROWTH) * handler->values_len;

  if (*fragment_id > MAX_NUM_FRAGMENTS) {
    traceEvent(TRACE_ERROR, "Internal error [%u > %u]", *fragment_id, MAX_NUM_FRAGMENTS);
    return -2;
  }

  if (handler->chunk.fragment[*fragment_id]) {
    // We already have the right fragment.  Do nothing.

  } else if (handler->chunk.load_on_demand || handler->read_only_mode) {
    // We should load the fragment.
    u_int32_t value_len;
    char str[32];
    void *value;

    // TODO: optionally, unload other fragments?

    snprintf(str, sizeof(str), "%u-%u", handler->chunk.time, *fragment_id);
    if(map_raw_get(handler, str, strlen(str), &value, &value_len) == -1)
      return(-1);

    uint32_t len = qlz_size_decompressed(value);

    handler->chunk.fragment[*fragment_id] = (u_int8_t*)malloc(len);
    if (!handler->chunk.fragment[*fragment_id]) {
      traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", len);
      return(-2);
    }

    qlz_decompress(value, handler->chunk.fragment[*fragment_id], &handler->state_decompress);
    //free(value);

    if (handler->chunk.num_fragments <= *fragment_id)
      handler->chunk.num_fragments = *fragment_id + 1;

  } else if (handler->chunk.growable) {
    // We should allocate a new fragment.
    u_int32_t frag_len = CHUNK_GROWTH * handler->values_len;
    u_int8_t *ptr    = malloc(frag_len);

    if (ptr) {
      memset(ptr, handler->default_unknown_value, frag_len);       
      handler->chunk.fragment[*fragment_id] = ptr;
      handler->chunk.num_fragments = *fragment_id + 1;

      traceEvent(TRACE_INFO, "Grown table to %u elements",
        handler->chunk.num_fragments * CHUNK_GROWTH);

    } else {
      traceEvent(TRACE_WARNING, "Not enough memory (%u bytes): unable to grow table", frag_len);
      return(-2);
    }

  } else {
    traceEvent(TRACE_ERROR, "Index %u out of range %u...%u",
	       key_index, 0,
	       handler->chunk.num_fragments * CHUNK_GROWTH - 1);
    return(-1);
  }

  return(0);
}

/* *********************************************************************** */

int tsdb_set(tsdb_handler *handler,
	     char *key,
	     tsdb_value *value_to_store) {
  u_int64_t offset;
  uint32_t fragment_id;

  if (!handler->is_open)
    return -1;

  if (getOffset(handler, key, &fragment_id, &offset, 1) == 0) {
    memcpy(handler->chunk.fragment[fragment_id] + offset,
	value_to_store, handler->values_len);
    handler->chunk.fragment_changed[fragment_id] = 1;

    traceEvent(TRACE_INFO, "Succesfully set value [offset=%" PRIu64 "][value_len=%u][fragment_id=%u]",
	       offset, handler->values_len, fragment_id);

  } else {
    traceEvent(TRACE_ERROR, "Missing time");
    return -2;
  }

  return 0;
}

/* *********************************************************************** */

int tsdb_get(tsdb_handler *handler,
	     char *key,
	     tsdb_value **value_to_read) {
  u_int64_t offset;
  uint32_t fragment_id;

  if(!handler->is_open) {
    *value_to_read = &handler->default_unknown_value;
    return -1;
  }

  if (getOffset(handler, key, &fragment_id, &offset, 0) == 0) {
    *value_to_read = (tsdb_value*)(handler->chunk.fragment[fragment_id] + offset);

    traceEvent(TRACE_INFO, "Succesfully read value [offset=%" PRIu64 "][value_len=%u]",
		 offset, handler->values_len);

  } else {
    traceEvent(TRACE_ERROR, "Missing time");
    *value_to_read = &handler->default_unknown_value;
    return -2;
  }

  return 0;
}

/* *********************************************************************** */

void tsdb_stat_print(tsdb_handler *handler) {
  int ret;
  
  if((ret = handler->db->stat_print(handler->db, DB_FAST_STAT)) != 0) {
    traceEvent(TRACE_ERROR, "Error while dumping DB stats [%s]", db_strerror(ret));
  }
}
