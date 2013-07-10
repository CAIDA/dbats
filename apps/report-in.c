#include <stdio.h>
#include <stdlib.h>
#include "tsdb_api.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {tsdb_path} < report.metrics\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{0|1|2|3}    verbosity level\n");
    fprintf(stderr, "-p             preload timeslices\n");
    fprintf(stderr, "-Z             don't compress db\n");
    exit(-1);
}

int main(int argc, char *argv[]) {
    char *tsdb_path;
    tsdb_handler handler;
    uint16_t num_values_per_entry = 1;
    uint32_t period = 60;
    int goto_flags = 0;
    int open_flags = TSDB_CREATE;
    progname = argv[0];

    //traceLevel = 99;
    traceLevel = 1;

    uint32_t last_t = 0;
    uint32_t t;
    tsdb_value value;
    char key[128];

    int c;
    while ((c = getopt(argc, argv, "v:pZ")) != -1) {
	switch (c) {
	case 'v':
	    traceLevel = atoi(optarg);
	    break;
	case 'p':
	    goto_flags |= TSDB_PRELOAD;
	    break;
	case 'Z':
	    open_flags |= TSDB_UNCOMPRESSED;
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
    tsdb_path = argv[0];

    if (tsdb_open(&handler, tsdb_path, num_values_per_entry, period, open_flags) != 0)
        return -1;
    if (tsdb_aggregate(&handler, TSDB_AGG_MIN, 10) != 0)
	return -1;
    if (tsdb_aggregate(&handler, TSDB_AGG_MAX, 10) != 0)
	return -1;
    if (tsdb_aggregate(&handler, TSDB_AGG_AVG, 10) != 0)
	return -1;
    if (tsdb_aggregate(&handler, TSDB_AGG_LAST, 10) != 0)
	return -1;
    if (tsdb_aggregate(&handler, TSDB_AGG_SUM, 10) != 0)
	return -1;

    while (1) {
	int n = scanf("%127s %" SCNval " %" SCNu32 "\n", key, &value, &t);
	if (n != 3) break;

	if (t != last_t) {
	    if (tsdb_goto_time(&handler, t, goto_flags) == -1)
		return(-1);
	}
	last_t = t;

	// XXX TODO: use tsdb_set() instead of tsdb_set_by_key()
	if (tsdb_set_by_key(&handler, key, &value) != 0)
	    return -1;
    }

    tsdb_close(&handler);

    return(0);
}
