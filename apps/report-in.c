#include <stdio.h>
#include <stdlib.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path} < report.metrics\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{0|1|2|3}    verbosity level\n");
    fprintf(stderr, "-p             preload timeslices\n");
    fprintf(stderr, "-Z             don't compress db\n");
    exit(-1);
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    uint32_t period = 60;
    int select_flags = 0;
    int open_flags = DBATS_CREATE;
    progname = argv[0];

    dbats_log_level = 1;

    uint32_t last_t = 0;
    uint32_t t;
    dbats_value value;
    char key[128];

    int c;
    while ((c = getopt(argc, argv, "v:pZ")) != -1) {
	switch (c) {
	case 'v':
	    dbats_log_level = atoi(optarg);
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

    handler = dbats_open(dbats_path, 1, period, open_flags);
    if (!handler) return -1;

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

    while (1) {
	int n = scanf("%127s %" SCNval " %" SCNu32 "\n", key, &value, &t);
	if (n != 3) break;

	if (t != last_t) {
	    if (dbats_select_time(handler, t, select_flags) == -1)
		return(-1);
	}
	last_t = t;

	// XXX TODO: use dbats_set() instead of dbats_set_by_key()
	if (dbats_set_by_key(handler, key, &value) != 0)
	    return -1;
    }

    dbats_close(handler);

    return(0);
}
