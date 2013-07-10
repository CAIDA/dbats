#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "tsdb_api.h"

static char *progname = 0;

/* *********************************** */

static void help(void) {
    fprintf(stderr, "%s [{options}] {tsdb_path} {begin} {end}\n",
	progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{0|1|2|3}    verbosity level\n");
    fprintf(stderr, "-k{path}       load list of keys from {path}\n");
    exit(-1);
}

/* ***************************************************************** */

#define MAX_KEYS 262144
static tsdb_key_info_t *keys[MAX_KEYS];
static int n_keys = 0;

static void load_keys(tsdb_handler *handler, const char *path)
{
    char line[128];
    FILE *keyfile = fopen(path, "r");
    if (!keyfile) {
	fprintf(stderr, "%s: %s\n", path, strerror(errno));
	exit(-1);
    }
    while (fgets(line, sizeof(line), keyfile)) {
	char *p = strchr(line, '\n');
	if (p) *p = 0;
	tsdb_get_key_info(handler, line, &keys[n_keys], 0);
	n_keys++;
    }
    if (ferror(keyfile)) {
	fprintf(stderr, "%s: %s\n", path, strerror(errno));
	exit(-1);
    }
}

static void get_keys(tsdb_handler *handler)
{
    tsdb_keywalk_start(handler);
    while (tsdb_keywalk_next(handler, &keys[n_keys]) == 0) {
	n_keys++;
    }
    tsdb_keywalk_end(handler);
}

int main(int argc, char *argv[]) {
    tsdb_handler handler;
    uint32_t begin = 0, end = 0;
    uint32_t run_start, elapsed;
    const uint32_t *values;
    char *tsdb_path = NULL;
    char *keyfile_path = NULL;
    FILE *out;
    progname = argv[0];
    traceLevel = 1;

    int c;
    while ((c = getopt(argc, argv, "v:k:")) != -1) {
	switch (c) {
	case 'v':
	    traceLevel = atoi(optarg);
	    break;
	case 'k':
	    keyfile_path = strdup(optarg);
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

    tsdb_path = argv[0];
    begin = atol(argv[1]);
    end = atol(argv[2]);

    traceEvent(TRACE_INFO, "begin=%"PRId32 " end=%"PRId32, begin, end);
    if ((begin <= 0) || (end < begin))
	help();

    traceEvent(TRACE_INFO, "Opening %s", tsdb_path);

    if (tsdb_open(&handler, tsdb_path, 0, 0, TSDB_READONLY) != 0)
	return(-1);

    if (keyfile_path)
	load_keys(&handler, keyfile_path);
    else
	get_keys(&handler);

    tzset();
    out = stdout;
    run_start = time(NULL);

    for (int agg_id = 0; agg_id < handler.num_aggs; agg_id++) {
	for (uint32_t t = begin; t <= end; t += handler.agg[agg_id].period) {
	    int rc;

	    if ((rc = tsdb_goto_time(&handler, t, 0)) == -1) {
		traceEvent(TRACE_INFO, "Unable to find time %u", t);
		continue;
	    }

	    for (int k = 0; k < n_keys; k++) {
		rc = tsdb_get(&handler, keys[k], &values, agg_id);
		if (rc != 0) {
		    fprintf(stdout, "error in tsdb_get(%s)\n", keys[k]->key);
		    break;
		}
		fprintf(out, "%s ", keys[k]->key);
		for (int j = 0; j < handler.num_values_per_entry; j++) {
		    fprintf(out, "%u ", values ? values[j] : 0);
		}
		fprintf(out, "%u %d\n", t, agg_id);
	    }
	}
    }

    elapsed = time(NULL) - run_start;

    traceEvent(TRACE_INFO, "Time elapsed: %u sec", elapsed);
    traceEvent(TRACE_INFO, "Closing %s", tsdb_path);
    tsdb_close(&handler);
    traceEvent(TRACE_INFO, "Done");
    return(0);
}
