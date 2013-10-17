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
    int func;
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

    for (func = DBATS_AGG_SUM; func > 0; func--) {
	if (strcmp(dbats_agg_func_label[func], funcstr) == 0)
	    break;
    }
    if (func == 0) {
	fprintf(stderr, "unknown function '%s'\n", funcstr);
	exit(-1);
    }

    int rc = dbats_open(&handler, dbats_path, 0, 0, open_flags, 0);
    if (rc != 0) return -1;
    dbats_commit_open(handler);

    int bid = dbats_best_bundle(handler, func, start, end, maxPoints);
    printf("best bundle: %d\n", bid);

    dbats_close(handler);

    return 0;
}
