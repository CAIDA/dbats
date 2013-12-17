#define _POSIX_C_SOURCE 200809L
#include <inttypes.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "uint.h"
#include <db.h> // for DB_LOCK_DEADLOCK
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path} < report.metrics\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}    verbosity level\n");
    fprintf(stderr, "-a{N}    define {N} aggregations\n");
    fprintf(stderr, "-x       obtain exclusive lock on db\n");
    fprintf(stderr, "-t       don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-u       allow updates to existing data\n");
    fprintf(stderr, "-p       preload timeslices\n");
    fprintf(stderr, "-Z       don't compress db\n");
    fprintf(stderr, "-i       initialize db only; do not write data\n");
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
    if (dbats_caught_signal) return EINTR;
    dbats_log(DBATS_LOG_INFO, "select time %u", t);
    rc = dbats_select_snap(handler, &snapshot, t, select_flags);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "error in dbats_select_snap()");
	return -1;
    }

    if (n_unknown_keys > 0) {
	dbats_log(DBATS_LOG_INFO, "dbats_bulk_get_key_id: %d unknown keys", n_unknown_keys);
	rc = dbats_bulk_get_key_id(handler, snapshot, n_keys, (const char * const *)keys, keyids, DBATS_CREATE);
	n_unknown_keys = 0;
    }

    for (int i = 0; i < n_keys; i++) {
	if (dbats_caught_signal) {
	    dbats_abort_snap(snapshot);
	    return EINTR;
	}
	rc = dbats_set(snapshot, keyids[i], &data[i]);
	if (rc != 0) {
	    dbats_abort_snap(snapshot);
	    if (rc == DB_LOCK_DEADLOCK) {
		dbats_log(DBATS_LOG_WARN, "deadlock in dbats_set()");
		goto retry;
	    }
	    dbats_log(DBATS_LOG_ERR, "error in dbats_set(): %s", db_strerror(rc));
	    return -1;
	}
    }
    rc = dbats_commit_snap(snapshot);
    if (rc != 0) {
	if (rc == DB_LOCK_DEADLOCK) {
	    dbats_log(DBATS_LOG_WARN, "deadlock in dbats_commit()");
	    goto retry;
	}
	dbats_log(DBATS_LOG_ERR, "error in dbats_commit(): %s", db_strerror(rc));
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
    int n_aggs = 0;

    dbats_log_level = DBATS_LOG_INFO;

    uint32_t last_t = 0;
    uint32_t t;
    dbats_value value;
    char key[128];

    int c;
    while ((c = getopt(argc, argv, "v:a:xtupZi")) != -1) {
	switch (c) {
	case 'v':
	    dbats_log_level = atoi(optarg);
	    break;
	case 'a':
	    n_aggs = atoi(optarg);
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
    dbats_log(DBATS_LOG_INFO, "report-in: open");
    dbats_catch_signals();
    if (dbats_open(&handler, dbats_path, 1, period, open_flags, 0644) != 0)
	return -1;

    const dbats_config *cfg = dbats_get_config(handler);
    if (cfg->num_bundles == 1) {
	for (int i = 0; i < n_aggs; i++) {
	    dbats_log(DBATS_LOG_INFO, "report-in: config aggs");
	    if (dbats_aggregate(handler, i%(DBATS_AGG_N-1) + 1, 10) != 0)
		return -1;
	}
    }
    dbats_commit_open(handler); // commit the txn started by dbats_open

    if (init_only) {
	while (1) {
	    int n = scanf("%127s %" SCNu64 " %" SCNu32 "\n", key, &value.u64, &t);
	    if (n != 3) break;

	    if (n_keys > 0 && t != last_t)
		break;
	    last_t = t;

	    keys[n_keys++] = strdup(key);
	}

	if (n_keys > 0) {
	    rc = dbats_bulk_get_key_id(handler, NULL, &n_keys,
		(const char * const *)keys, keyids, DBATS_CREATE);
	    dbats_log(DBATS_LOG_INFO, "Initialized %d keys", n_keys);
	}

    } else {
	while (1) {
	    int n = scanf("%127s %" SCNu64 " %" SCNu32 "\n", key, &value.u64, &t);
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

    dbats_log(DBATS_LOG_INFO, "Time elapsed: %u sec", elapsed);
    dbats_log(DBATS_LOG_INFO, "Closing %s", dbats_path);
    if (dbats_close(handler) != 0)
	return -1;
    dbats_deliver_signal(); // if signal was caught, exit as if it was uncaught
    dbats_log(DBATS_LOG_INFO, "Done");
    return rc ? 1 : 0;
}
