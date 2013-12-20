#define _POSIX_C_SOURCE 200809L
#include <unistd.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path} {min|max|avg|last|sum} {start} [{end} {maxPoints}]\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}    verbosity level\n");
    fprintf(stderr, "-x       obtain exclusive lock on db\n");
    fprintf(stderr, "-t       don't use transactions (fast, but unsafe)\n");
    exit(-1);
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    int open_flags = DBATS_READONLY;
    progname = argv[0];

    dbats_log_level = DBATS_LOG_INFO;

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

    if (argc != 3 && argc != 5)
	help();
    dbats_path = argv[0];
    const char *funcstr = argv[1];
    uint32_t start = atol(argv[2]);
    uint32_t end = 0;
    int maxPoints = 0;
    if (argc == 5) {
	end = atol(argv[3]);
	maxPoints = atoi(argv[4]);
    }

    enum dbats_agg_func func = dbats_find_agg_func(funcstr);
    if (func == DBATS_AGG_NONE) {
	fprintf(stderr, "unknown function '%s'\n", funcstr);
	exit(-1);
    }

    dbats_catch_signals();
    int rc = dbats_open(&handler, dbats_path, 0, 0, open_flags, 0);
    if (rc != 0) return -1;
    dbats_commit_open(handler);

    int bid = dbats_best_bundle(handler, func, start, end, maxPoints, 0);
    printf("best bundle: %d\n", bid);

    dbats_close(handler);

    dbats_deliver_signal(); // if signal was caught, exit as if it was uncaught
    return 0;
}
