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
#include <fcntl.h>
#include <ctype.h>
#include <limits.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <db.h>
#include <errno.h>
#include <sys/file.h>

#include "tsdb_trace.h"
#include "quicklz.h"

/* ************************************************** */

#define ENTRIES_PER_FRAG    10000 // number of entries in a fragment
#define MAX_NUM_FRAGS       16384 // max number of fragments in a tslice
#define MAX_NUM_AGGLVLS        16 // max number of aggregation levels

// Flags
#define TSDB_CREATE          0x01
#define TSDB_GROWABLE        0x02
#define TSDB_LOAD_ON_DEMAND  0x04

typedef struct tsdb_frag tsdb_frag;

// Logical row or "time slice", containing all the entries for a given time.
// Entries are actually batched into fragments within a tslice.
typedef struct {
    uint32_t time;                        // start time (unix seconds)
    uint32_t num_frags;                   // number of fragments
    uint8_t growable;                     // new fragments can be appended
    uint8_t load_on_demand;               // don't load fragment until needed
    tsdb_frag *frag[MAX_NUM_FRAGS];
    uint8_t frag_changed[MAX_NUM_FRAGS];
} tsdb_tslice;

#define TSDB_AGG_NONE   0
#define TSDB_AGG_MIN    1
#define TSDB_AGG_MAX    2
#define TSDB_AGG_AVG    3
#define TSDB_AGG_LAST   4
#define TSDB_AGG_SUM    5

// Aggregation parameters
typedef struct {
    int func;               // aggregation function
    int steps;              // number of primary data points in agg
    uint32_t period;       // length of slice (seconds)
} tsdb_agg;

typedef uint32_t tsdb_value;

typedef struct {
    uint8_t  is_open;
    uint8_t  readonly;                   // Mode used to open the db file
    uint16_t num_agglvls;                // Number of aggregation levels
    uint16_t num_values_per_entry;       // Number of tsdb_values in an entry
    uint16_t entry_size;                 // Size of an entry (bytes)
    uint32_t lowest_free_index;          // Hint to speed finding a free index
    uint32_t rrd_slot_time_duration;     // length of raw time slice (sec)
    qlz_state_compress state_compress;
    qlz_state_decompress state_decompress;
    DB_ENV *dbenv;                        // DB environment
    DB *dbMeta;                           // config parameters
    DB *dbKey;                            // metric key -> index
    DB *dbIndex;                          // index -> metric key
    DB *dbData;                           // {time, agglvl, frag_id} -> data
    DBC *keywalk;                         // cursor for iterating over keys
    tsdb_tslice tslice[MAX_NUM_AGGLVLS];  // a tslice for each aggregation level
    tsdb_agg agg[MAX_NUM_AGGLVLS];        // parameters for each aggregation level
} tsdb_handler;

/* ************************************************** */

extern int tsdb_open(const char *tsdb_path, tsdb_handler *handler,
    uint16_t num_values_per_entry,
    uint32_t rrd_slot_time_duration,
    uint8_t readonly);

extern int tsdb_aggregate(tsdb_handler *handler, int func, int steps);

extern void tsdb_close(tsdb_handler *handler);

extern uint32_t normalize_time(const tsdb_handler *handler, int s, uint32_t *time);

extern int tsdb_goto_time(tsdb_handler *handler,
    uint32_t time_value, uint32_t flags);

extern int tsdb_set(tsdb_handler *handler,
    const char *key, const tsdb_value *valuep);

extern int tsdb_get(tsdb_handler *handler,
    const char *key, const tsdb_value **valuepp, int agglvl);

extern int tsdb_keywalk_start(tsdb_handler *handler);
extern int tsdb_keywalk_next(tsdb_handler *handler, char **key, int *len);
extern int tsdb_keywalk_end(tsdb_handler *handler);

extern void tsdb_stat_print(const tsdb_handler *handler);
