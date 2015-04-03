#define _POSIX_C_SOURCE 200809L
#include <unistd.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "uint.h"
#include <db.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path} {pattern}\n", progname);
    fprintf(stderr, "Delete keys matching {pattern} from a DBATS database.\n");
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}       verbosity level\n");
    fprintf(stderr, "-x          obtain exclusive lock on db\n");
    fprintf(stderr, "-t          don't use transactions (fast, but unsafe)\n");
    exit(-1);
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    int open_flags = DBATS_UPDATABLE;
    char *keyglob;
    progname = argv[0];
    int rc = 0;

    int c;
    while ((c = getopt(argc, argv, "v:xt")) != -1) {
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
	default:
	    help();
	    break;
	}
    }
    argv += optind;
    argc -= optind;

    if (argc != 2)
	help();
    dbats_path = argv[0];
    keyglob = argv[1];

    dbats_catch_signals();
    if (dbats_open(&handler, dbats_path, 1, 0, open_flags, 0) != 0)
	return -1;
    dbats_commit_open(handler);

    rc = dbats_delete_keys(handler, keyglob);
    if (rc != 0) {
	dbats_log(DBATS_LOG_ERR, "internal error in dbats_delete_keys: %s",
	    db_strerror(rc));
    }

    dbats_close(handler);

    dbats_deliver_signal(); // if a signal was caught, exit as if it weren't
    return rc ? 1 : 0;
}
