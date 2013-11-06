#define _POSIX_C_SOURCE 200809L
#include <unistd.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path}\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}    verbosity level\n");
    fprintf(stderr, "-x       obtain exclusive lock on db\n");
    fprintf(stderr, "-t       don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-p       preload timeslices\n");
    fprintf(stderr, "-Z       don't compress db\n");
    exit(-1);
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    int select_flags = 0;
    int open_flags = DBATS_UPDATABLE;
    int rc = 0;
    uint32_t t;
    dbats_snapshot *snap = NULL;
    dbats_keyid_iterator *dki;
    uint32_t keyid;
    char keybuf[DBATS_KEYLEN];
    dbats_value value;
    value.u64 = 0xDEADBEEF;

    progname = argv[0];
    dbats_log_level = DBATS_LOG_INFO;

    int c;
    while ((c = getopt(argc, argv, "v:xtpZ")) != -1) {
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

    rc = dbats_open(&handler, dbats_path, 1, 0, open_flags, 0);
    if (rc != 0) return -1;

    rc = dbats_commit_open(handler); // commit the txn started by dbats_open
    if (rc != 0) goto cleanup_db;

    rc = dbats_walk_keyid_start(handler, snap, &dki);
    if (rc != 0) goto cleanup_db;
    rc = dbats_walk_keyid_next(dki, &keyid, keybuf);
    if (rc != 0) goto cleanup_cursor;

    rc = dbats_get_end_time(handler, NULL, 0, &t);
    if (rc != 0) goto cleanup_cursor;
    rc = dbats_select_snap(handler, &snap, t, select_flags);
    if (rc != 0) goto cleanup_cursor;
    rc = dbats_set(snap, keyid, &value);
    if (rc != 0) goto cleanup_snap;

cleanup_snap:
    //dbats_abort_snap(snap);
cleanup_cursor:
    //dbats_walk_keyid_end(dki);
cleanup_db:
    //dbats_close(handler);

    // don't close cursor or snapshot or db
    return rc;
}
