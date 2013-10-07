#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <db.h> // for DB_LOCK_DEADLOCK
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path} < report.metrics\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}    verbosity level\n");
    fprintf(stderr, "-x       obtain exclusive lock on db\n");
    fprintf(stderr, "-t       don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-u       allow updates to existing data\n");
    fprintf(stderr, "-p       preload timeslices\n");
    fprintf(stderr, "-Z       don't compress db\n");
    fprintf(stderr, "-i       initialize db only; do not write data\n");
    fprintf(stderr, "-m       allow multiple processes to write simultaneously (experimental)\n");
    exit(-1);
}

char *keys[10000000];
uint32_t keyids[10000000];
dbats_value data[10000000];
uint32_t n_keys = 0;
uint32_t n_unknown_keys = 0;

static int write_data(dbats_handler *handler, int select_flags, uint32_t t)
{
    int rc;
    dbats_snapshot *snapshot;

retry:
    dbats_log(LOG_INFO, "select time %u", t);
    rc = dbats_select_snap(handler, &snapshot, t, select_flags);
    if (rc != 0) {
	dbats_log(LOG_ERROR, "error in dbats_select_snap()");
	return -1;
    }

    if (n_unknown_keys > 0) {
	dbats_log(LOG_INFO, "dbats_bulk_get_key_id: %d unknown keys", n_unknown_keys);
	rc = dbats_bulk_get_key_id(handler, snapshot, n_keys, (const char * const *)keys, keyids, DBATS_CREATE);
	n_unknown_keys = 0;
    }

    for (int i = 0; i < n_keys; i++) {
	rc = dbats_set(snapshot, keyids[i], &data[i]);
	if (rc != 0) {
	    dbats_abort_snap(snapshot);
	    if (rc == DB_LOCK_DEADLOCK) {
		dbats_log(LOG_WARNING, "deadlock in dbats_set()");
		goto retry;
	    }
	    dbats_log(LOG_ERROR, "error in dbats_set(): %s", db_strerror(rc));
	    return -1;
	}
    }
    rc = dbats_commit_snap(snapshot);
    if (rc != 0) {
	if (rc == DB_LOCK_DEADLOCK) {
	    dbats_log(LOG_WARNING, "deadlock in dbats_commit()");
	    goto retry;
	}
	dbats_log(LOG_ERROR, "error in dbats_commit(): %s", db_strerror(rc));
	return -1;
    }
    n_keys = 0;
    return 0;
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    uint32_t period = 60;
    int select_flags = 0;
    int open_flags = DBATS_CREATE;
    int init_only = 0;
    progname = argv[0];
    uint32_t run_start, elapsed;
    int rc = 0;

    dbats_log_level = LOG_INFO;

    uint32_t last_t = 0;
    uint32_t t;
    dbats_value value;
    char key[128];

    int c;
    while ((c = getopt(argc, argv, "v:xtupZim")) != -1) {
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
	case 'u':
	    open_flags |= DBATS_UPDATABLE;
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

    if (init_only) {
	while (1) {
	    int n = scanf("%127s %" SCNval " %" SCNu32 "\n", key, &value, &t);
	    if (n != 3) break;

	    if (n_keys > 0 && t != last_t)
		break;
	    last_t = t;

	    keys[n_keys++] = strdup(key);
	}

	if (n_keys > 0) {
	    rc = dbats_bulk_get_key_id(handler, NULL, n_keys, (const char * const *)keys, keyids, DBATS_CREATE);
	}
	dbats_commit_open(handler); // commit the txn started by dbats_open

    } else {
	dbats_commit_open(handler); // commit the txn started by dbats_open
	while (1) {
	    int n = scanf("%127s %" SCNval " %" SCNu32 "\n", key, &value, &t);
	    if (n != 3) break;

	    if (t != last_t) {
		if (n_keys > 0) {
		    rc = write_data(handler, select_flags, last_t);
		    if (rc != 0) goto done;
		}
	    }
	    last_t = t;

	    if (!keys[n_keys] || strcmp(keys[n_keys], key) != 0) {
		// If input keys are in consistent order, this will only
		// happen in the first round.
		keyids[n_keys] = UINT32_MAX;
		n_unknown_keys++;
		if (keys[n_keys]) free(keys[n_keys]); // shouldn't happen
		keys[n_keys] = strdup(key);
	    }

	    data[n_keys] = value;
	    n_keys++;
	}

	if (n_keys > 0) {
	    rc = write_data(handler, select_flags, last_t);
	}
    }

done:
    elapsed = time(NULL) - run_start;

    dbats_log(LOG_INFO, "Time elapsed: %u sec", elapsed);
    dbats_log(LOG_INFO, "Closing %s", dbats_path);
    if (dbats_close(handler) != 0)
	return -1;
    dbats_log(LOG_INFO, "Done");
    return rc;
}
