/*
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>
#include <strings.h>
#include <limits.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h> /* mmap() */
#include <stdarg.h>
#include <sys/stat.h>
#include <db.h>
#include <errno.h>
#include <sys/file.h>

#include "tsdb_trace.h"
#include "quicklz.h"

/* ************************************************** */

//#define ENTRIES_PER_FRAG        100000
#define ENTRIES_PER_FRAG        10000
#define MAX_NUM_FRAGMENTS      16384

// Flags
#define TSDB_CREATE          0x01
#define TSDB_GROWABLE        0x02
#define TSDB_LOAD_ON_DEMAND  0x04

// Logical row or "time slice", containing all the entries for a given time.
// Entries are actually batched into fragments within a tslice.
typedef struct {
    u_int8_t *fragment[MAX_NUM_FRAGMENTS];
    u_int32_t time;
    u_int8_t growable;
    u_int32_t num_fragments;
    u_int8_t fragment_changed[MAX_NUM_FRAGMENTS];
    u_int8_t load_on_demand;
} tsdb_tslice;

typedef u_int32_t tsdb_value;

typedef struct {
    u_int8_t  is_open;
    u_int8_t  read_only_mode;              // Mode used to open the db file
    u_int16_t num_values_per_entry;        // Number of tsdb_values per entry
    u_int16_t entry_size;                  // Size of an entry (bytes)
    u_int32_t default_unknown_value;       // Default 0
    u_int32_t lowest_free_index;           // Hint to speed finding a free index
    u_int32_t rrd_slot_time_duration;      // length of time slice (sec)
    qlz_state_compress state_compress;     //
    qlz_state_decompress state_decompress; //

    tsdb_tslice tslice;

    DB *db;
} tsdb_handler;

typedef struct {
    u_int32_t time_start; // First time in which this mapping is valid
    u_int32_t time_end;   // Last time in which this mapping is valid
    u_int32_t key_idx;
} tsdb_key_mapping;

/* ************************************************** */

extern int tsdb_open(const char *tsdb_path, tsdb_handler *handler,
    u_int16_t num_values_per_entry,
    u_int32_t rrd_slot_time_duration,
    u_int8_t read_only_mode);

extern void tsdb_close(tsdb_handler *handler);

extern u_int32_t normalize_time(const tsdb_handler *handler, u_int32_t *time);

extern int tsdb_goto_time(tsdb_handler *handler,
    u_int32_t time_value, uint32_t flags);

extern int tsdb_set(tsdb_handler *handler,
    const char *key, const tsdb_value *value_to_store);

extern int tsdb_get(tsdb_handler *handler,
    const char *key, const tsdb_value **value_to_read);

extern void tsdb_drop_key(const tsdb_handler *handler,
    const char *key, u_int32_t time_value);

extern void tsdb_stat_print(const tsdb_handler *handler);
