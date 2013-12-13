#define _POSIX_C_SOURCE 200809L
#include <unistd.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path}\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}    verbosity level\n");
    exit(-1);
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    progname = argv[0];
    uint32_t run_start, elapsed;
    int rc = 0;

    dbats_log_level = DBATS_LOG_INFO;

    int c;
    while ((c = getopt(argc, argv, "v")) != -1) {
	switch (c) {
	case 'v':
	    dbats_log_level = atoi(optarg);
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
    dbats_catch_signals();
    if (dbats_open(&handler, dbats_path, 1, 0, 0, 0644) != 0)
	return -1;
    if (dbats_caught_signal)
	dbats_abort_open(handler); // abort the txn started by dbats_open
    else
	dbats_commit_open(handler); // commit the txn started by dbats_open

    elapsed = time(NULL) - run_start;
    dbats_log(DBATS_LOG_INFO, "Time elapsed: %u sec", elapsed);
    dbats_log(DBATS_LOG_INFO, "Closing %s", dbats_path);
    if (dbats_close(handler) != 0)
	return -1;
    dbats_deliver_signal(); // if signal was caught, exit as if it was uncaught
    dbats_log(DBATS_LOG_INFO, "Done");
    return rc;
}
