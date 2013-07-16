#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "dbats.h"

static char *progname = 0;

/* *********************************** */

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path}\n", progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{0|1|2|3}    verbosity level\n");
    fprintf(stderr, "-k{path}       load list of keys from {path}\n");
    fprintf(stderr, "               (default: use all keys in db)\n");
    fprintf(stderr, "-b{begin}      begin time\n");
    fprintf(stderr, "               (default: first time in db)\n");
    fprintf(stderr, "-e{end}        end time\n");
    fprintf(stderr, "               (default: last time in db)\n");
    exit(-1);
}

/* ***************************************************************** */

#define MAX_KEYS 262144
static uint32_t key_id[MAX_KEYS];
static int n_keys = 0;

static void load_keys(dbats_handler *handler, const char *path)
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
	if (dbats_get_key_id(handler, line, &key_id[n_keys], 0) != 0) {
	    fprintf(stderr, "no such key: %s\n", line);
	    exit(-1);
	}
	n_keys++;
    }
    if (ferror(keyfile)) {
	fprintf(stderr, "%s: %s\n", path, strerror(errno));
	exit(-1);
    }
}

static void get_keys(dbats_handler *handler)
{
    dbats_walk_keyid_start(handler);
    while (dbats_walk_keyid_next(handler, &key_id[n_keys]) == 0) {
	n_keys++;
    }
    dbats_walk_keyid_end(handler);
}

int main(int argc, char *argv[]) {
    dbats_handler handler;
    uint32_t begin = 0, end = 0;
    uint32_t run_start, elapsed;
    char *dbats_path = NULL;
    char *keyfile_path = NULL;
    FILE *out;
    progname = argv[0];
    dbats_log_level = 1;

    int c;
    while ((c = getopt(argc, argv, "v:k:b:e:")) != -1) {
	switch (c) {
	case 'v':
	    dbats_log_level = atoi(optarg);
	    break;
	case 'k':
	    keyfile_path = strdup(optarg);
	    break;
	case 'b':
	    begin = atol(optarg);
	    break;
	case 'e':
	    end = atol(optarg);
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

    dbats_log(LOG_INFO, "begin=%"PRId32 " end=%"PRId32, begin, end);
    if (end < begin)
	help();

    dbats_log(LOG_INFO, "Opening %s", dbats_path);

    if (dbats_open(&handler, dbats_path, 0, 0, DBATS_READONLY) != 0)
	return(-1);

    const dbats_timerange_t *times = dbats_get_agg_times(&handler, 0);
    if (begin == 0) begin = times->start;
    if (end == 0) end = times->end;

    if (keyfile_path)
	load_keys(&handler, keyfile_path);
    else
	get_keys(&handler);

    tzset();
    out = stdout;
    run_start = time(NULL);

    uint32_t values_per_entry = dbats_get_values_per_entry(&handler);
    uint32_t num_aggs = dbats_get_num_aggs(&handler);

    for (int agg_id = 0; agg_id < num_aggs; agg_id++) {
	for (uint32_t t = begin; t <= end; t += dbats_get_agg_period(&handler, agg_id)) {
	    int rc;

	    if ((rc = dbats_goto_time(&handler, t, 0)) == -1) {
		dbats_log(LOG_INFO, "Unable to find time %u", t);
		continue;
	    }

	    if (dbats_get_agg_func(&handler, agg_id) == DBATS_AGG_AVG) {
		const double *values;
		for (int k = 0; k < n_keys; k++) {
		    const char *key = dbats_get_key_name(&handler, key_id[k]);
		    rc = dbats_get_double(&handler, key_id[k], &values, agg_id);
		    if (rc != 0) {
			fprintf(stdout, "error in dbats_get(%s)\n", key);
			break;
		    }
		    fprintf(out, "%s ", key);
		    for (int j = 0; j < values_per_entry; j++) {
			fprintf(out, "%.3f ", values ? values[j] : 0);
		    }
		    fprintf(out, "%u %d\n", t, agg_id);
		}
	    } else {
		const dbats_value *values;
		for (int k = 0; k < n_keys; k++) {
		    const char *key = dbats_get_key_name(&handler, key_id[k]);
		    rc = dbats_get(&handler, key_id[k], &values, agg_id);
		    if (rc != 0) {
			fprintf(stdout, "error in dbats_get(%s)\n", key);
			break;
		    }
		    fprintf(out, "%s ", key);
		    for (int j = 0; j < values_per_entry; j++) {
			fprintf(out, "%" PRIval " ", values ? values[j] : 0);
		    }
		    fprintf(out, "%u %d\n", t, agg_id);
		}
	    }
	}
    }

    elapsed = time(NULL) - run_start;

    dbats_log(LOG_INFO, "Time elapsed: %u sec", elapsed);
    dbats_log(LOG_INFO, "Closing %s", dbats_path);
    dbats_close(&handler);
    dbats_log(LOG_INFO, "Done");
    return(0);
}
