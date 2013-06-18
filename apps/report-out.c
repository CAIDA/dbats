#include "tsdb_api.h"

static char *progname = 0;

/* *********************************** */

static void help(void) {
    fprintf(stderr, "%s [{options}] {tsdb_path} {keylist_path} {begin} {end}\n",
	progname);
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{0|1|2|3}    verbosity level\n");
    exit(-1);
}

/* ***************************************************************** */

#define MAX_KEYS 262144
static char *key[MAX_KEYS];
static int n_key = 0;

static void load_keys(const char *path)
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
	key[n_key++] = strdup(line);
    }
    if (ferror(keyfile)) {
	fprintf(stderr, "%s: %s\n", path, strerror(errno));
	exit(-1);
    }
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

    int c;
    while ((c = getopt(argc, argv, "v:")) != -1) {
	switch (c) {
	case 'v':
	    traceLevel = atoi(optarg);
	    break;
	default:
	    help();
	    break;
	}
    }
    argv += optind;
    argc -= optind;


    if (argc != 4)
	help();

    tsdb_path = argv[0];
    keyfile_path = argv[1];
    begin = atol(argv[2]);
    end = atol(argv[3]);

    traceEvent(TRACE_INFO, "begin=%"PRId32 " end=%"PRId32, begin, end);
    if ((begin <= 0) || (end < begin))
	help();

    load_keys(keyfile_path);

    traceEvent(TRACE_INFO, "Opening %s", tsdb_path);

    if (tsdb_open(tsdb_path, &handler, 0, 0, 1) != 0)
	return(-1);

    tzset();
    out = stdout;
    run_start = time(NULL);

    for (int agglvl = 0; agglvl < handler.num_agglvls; agglvl++) {
	for (uint32_t t = begin; t <= end; t += handler.agg[agglvl].period) {
	    int rc;

	    if ((rc = tsdb_goto_time(&handler, t, TSDB_LOAD_ON_DEMAND)) == -1) {
		traceEvent(TRACE_INFO, "Unable to find time %u", t);
		continue;
	    }

	    for (int k = 0; k < n_key; k++) {
		rc = tsdb_get(&handler, key[k], &values, agglvl);
		if (rc < 0) {
		    fprintf(stdout, "error in tsdb_get(%s)\n", key[k]);
		    continue;
		}
		fprintf(out, "%s ", key[k]);
		for (int j = 0; j < handler.num_values_per_entry; j++) {
		    fprintf(out, "%u ", values ? values[j] : 0);
		}
		fprintf(out, "%u %d\n", t, agglvl);
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
