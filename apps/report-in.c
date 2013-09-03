#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path} < report.metrics\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}    verbosity level\n");
    fprintf(stderr, "-x       obtain exclusive lock on db\n");
    fprintf(stderr, "-t       don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-p       preload timeslices\n");
    fprintf(stderr, "-Z       don't compress db\n");
    fprintf(stderr, "-i       initialize db only; do not write data\n");
    fprintf(stderr, "-m       allow multiple processes to write simultaneously (experimental)\n");
    exit(-1);
}

char *keys[10000000];

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    uint32_t period = 60;
    int select_flags = 0;
    int open_flags = DBATS_CREATE;
    int init_only = 0;
    progname = argv[0];
    uint32_t run_start, elapsed;

    dbats_log_level = LOG_INFO;

    uint32_t last_t = 0;
    uint32_t t;
    dbats_value value;
    char key[128];

    int c;
    while ((c = getopt(argc, argv, "v:xtpZim")) != -1) {
	switch (c) {
	case 'v':
	    dbats_log_level = atoi(optarg);
	    break;
	case 'x':
	    open_flags |= DBATS_EXCLUSIVE;
	    break;
	case 't':
	    open_flags |= DBATS_NO_TXN;
	    break;
	case 'p':
	    select_flags |= DBATS_PRELOAD;
	    break;
	case 'Z':
	    open_flags |= DBATS_UNCOMPRESSED;
	    break;
	case 'm':
	    open_flags |= DBATS_MULTIWRITE;
	    break;
	case 'i':
	    init_only = 1;
	    break;
	default:
	    help();
	    break;
	}
    }
    argv += optind;
    argc -= optind;

    if (argc != 1)
	help();
    dbats_path = argv[0];

    run_start = time(NULL);
    dbats_log(LOG_INFO, "report-in: open");
    handler = dbats_open(dbats_path, 1, period, open_flags);
    if (!handler) return -1;

    const dbats_config *cfg = dbats_get_config(handler);
    if (cfg->num_bundles == 1) {
	dbats_log(LOG_INFO, "report-in: config aggs");
	if (dbats_aggregate(handler, DBATS_AGG_MIN, 10) != 0)
	    return -1;
	if (dbats_aggregate(handler, DBATS_AGG_MAX, 10) != 0)
	    return -1;
	if (dbats_aggregate(handler, DBATS_AGG_AVG, 10) != 0)
	    return -1;
	if (dbats_aggregate(handler, DBATS_AGG_LAST, 10) != 0)
	    return -1;
	if (dbats_aggregate(handler, DBATS_AGG_SUM, 10) != 0)
	    return -1;
    }
    if (!init_only) dbats_commit(handler); // commit the txn started by dbats_open

    uint32_t key_id = 0;
    while (1) {
	int n = scanf("%127s %" SCNval " %" SCNu32 "\n", key, &value, &t);
	if (n != 3) break;

	if (t != last_t) {
	    if (!init_only) {
		dbats_log(LOG_INFO, "select time %u", t);
		if (dbats_select_time(handler, t, select_flags) != 0)
		    return(-1);
	    }
	    key_id = 0;
	}
	last_t = t;

	if (!keys[key_id] || strcmp(keys[key_id], key) != 0) {
	    // If input keys are in consistent order, this lookup will be
	    // skipped after the first round.
	    if (dbats_get_key_id(handler, key, &key_id, DBATS_CREATE) != 0)
		return -1;
	    if (keys[key_id]) free(keys[key_id]); // shouldn't happen
	    keys[key_id] = strdup(key);
	}

	if (!init_only) {
	    if (dbats_set(handler, key_id, &value) != 0)
		return -1;
	}
	key_id++;
    }

    elapsed = time(NULL) - run_start;

    dbats_log(LOG_INFO, "Time elapsed: %u sec", elapsed);
    dbats_log(LOG_INFO, "Closing %s", dbats_path);
    dbats_close(handler);
    dbats_log(LOG_INFO, "Done");
    return 0;
}
