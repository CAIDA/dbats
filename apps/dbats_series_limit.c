#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path} {series_id} {keep}\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}    verbosity level\n");
    fprintf(stderr, "-x       obtain exclusive lock on db\n");
    fprintf(stderr, "-t       don't use transactions (fast, but unsafe)\n");
    exit(-1);
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    uint32_t period = 60;
    int open_flags = 0;
    progname = argv[0];

    dbats_log_level = LOG_INFO;

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

    if (argc != 3)
	help();
    dbats_path = argv[0];
    int series_id = atoi(argv[1]);
    int keep = atoi(argv[2]);

    dbats_log(LOG_INFO, "%s: open", progname);
    handler = dbats_open(dbats_path, 1, period, open_flags);
    if (!handler) return -1;

    int rc = dbats_series_limit(handler, series_id, keep);

    dbats_close(handler);

    return rc;
}
