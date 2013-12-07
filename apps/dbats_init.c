#define _POSIX_C_SOURCE 200809L
#include <inttypes.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "uint.h"
#include <db.h> // for DB_LOCK_DEADLOCK
#include "dbats.h"

static char *progname = 0;
static uint32_t period = 60;
static uint16_t values_per_entry = 1;

static void help(void) {
    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "%s [{options}] {dbats_path} [{func}:{steps}[:{keep}]]...\n",
	progname);
    fprintf(stderr, "Create a DBATS database and optionally define aggregates "
	"and keys.\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "-v{N}     verbosity level\n");
    fprintf(stderr, "-t        don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-p{N}     data sampling period, in seconds [%d]\n", period);
    fprintf(stderr, "-e{N}     values per entry [%d]\n", values_per_entry);
    fprintf(stderr, "-k{FILE}  define keys listed in {FILE}\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "{func} is any of the aggregation functions: ");
    for (enum dbats_agg_func i = 1; i < DBATS_AGG_N; i++)
	fprintf(stderr, "%s%s", i==1 ? "" : ", ", dbats_agg_func_label[i]);
    fprintf(stderr, ".\n");
    fprintf(stderr, "{steps} is the number of primary data points contributing to one aggregate value.\n");
    fprintf(stderr, "{keep} is the number of aggregate data points to keep.\n");
    exit(-1);
}

#define MAX_KEYS 10000000
char *keys[MAX_KEYS];
uint32_t keyids[MAX_KEYS];
uint32_t n_keys = 0;

int main(int argc, char *argv[]) {
    char *dbats_path = NULL;
    char *keyfile = NULL;
    dbats_handler *handler;
    int open_flags = DBATS_CREATE;
    progname = argv[0];
    uint32_t run_start, elapsed;
    int rc = 0;

    dbats_log_level = DBATS_LOG_INFO;

    int c;
    while ((c = getopt(argc, argv, "v:tp:e:k:")) != -1) {
	switch (c) {
	case 'v':
	    dbats_log_level = atoi(optarg);
	    break;
	case 't':
	    open_flags |= DBATS_NO_TXN;
	    break;
	case 'p':
	    period = atoi(optarg);
	    if (period <= 0) {
		fprintf(stderr, "error: illegal -p value: %s\n", optarg);
		help();
	    }
	    break;
	case 'e':
	    values_per_entry = atoi(optarg);
	    if (values_per_entry <= 0) {
		fprintf(stderr, "error: illegal -e value: %s\n", optarg);
		help();
	    }
	    break;
	case 'k':
	    keyfile = strdup(optarg);
	    break;
	default:
	    help();
	    break;
	}
    }
    argv += optind;
    argc -= optind;

    if (argc < 1) {
	fprintf(stderr, "error: missing dbats_path\n");
	help();
    }
    dbats_path = argv[0];
    argv++;
    argc--;

    char funcname[10];
    enum dbats_agg_func *aggfunc = malloc(argc * sizeof(enum dbats_agg_func));
    int *aggsteps = malloc(argc * sizeof(int));
    int *aggkeep = malloc(argc * sizeof(int));
    for (int i = 0; i < argc; i++) {
	char colon = ':';
	char dummy = '\0';
	int n = sscanf(argv[i], "%9[A-Za-z0-9_]:%d%c%d%c", funcname, &aggsteps[i], &colon, &aggkeep[i], &dummy);
	fprintf(stderr, "agg: \"%s\" colon=%x dummy=%x n=%d\n", argv[i], colon, dummy, n);
	if ((n != 2 && n != 4) || colon != ':') {
	    fprintf(stderr, "Syntax error in aggregate specification \"%s\"\n", argv[i]);
	    help();
	}
	if (n < 4) aggkeep[i] = 0;
	aggfunc[i] = dbats_find_agg_func(funcname);
	if (aggfunc[i] == DBATS_AGG_NONE) {
	    fprintf(stderr, "no such function: %s\n", funcname);
	    return -1;
	}
    }

    run_start = time(NULL);
    dbats_log(DBATS_LOG_INFO, "%s: open", progname);
    if (dbats_open(&handler, dbats_path, values_per_entry, period, open_flags, 0644) != 0)
	return -1;

    const dbats_config *cfg = dbats_get_config(handler);

    for (int i = 0; i < argc; i++) {
	int bid;
	const dbats_bundle_info *bundle;
	for (bid = 0; bid < cfg->num_bundles; bid++) {
	    bundle = dbats_get_bundle_info(handler, bid);
	    if (bundle->func == aggfunc[i] && bundle->steps == aggsteps[i]) {
		break;
	    }
	}
	if (bid == cfg->num_bundles) {
	    // new aggregate
	    dbats_log(DBATS_LOG_INFO, "defining aggregate #%d %s:%d:%d",
		bid, dbats_agg_func_label[aggfunc[i]],
		aggsteps[i], aggkeep[i]);
	    if ((dbats_aggregate(handler, aggfunc[i], aggsteps[i])) != 0)
		return -1;
	} else if (aggkeep[i] == bundle->keep) {
	    // duplicate existing aggregate
	    dbats_log(DBATS_LOG_INFO, "skipping duplicate aggregate #%d %s:%d:%d",
		bid, dbats_agg_func_label[bundle->func], bundle->steps, bundle->keep);
	    continue;
	} else {
	    // changing {keep} of existing aggregate
	    dbats_log(DBATS_LOG_INFO, "redefining aggregate #%d %s:%d:%d",
		bid, dbats_agg_func_label[aggfunc[i]],
		aggsteps[i], aggkeep[i]);
	}
	if (dbats_series_limit(handler, bid, aggkeep[i]) != 0)
	    return -1;
	bundle = dbats_get_bundle_info(handler, bid);
    }

    if (keyfile) {
	FILE *f = fopen(keyfile, "r");
	if (!f) {
	    fprintf(stderr, "%s: %s\n", keyfile, strerror(errno));
	    return -1;
	}
	char line[DBATS_KEYLEN+1];
	int linenum = 0;
	while (fgets(line, sizeof(line), f)) {
	    linenum++;
	    if (n_keys >= MAX_KEYS) {
		fprintf(stderr, "%s:%d: %s\n", keyfile, linenum, "too many keys");
		return -1;
	    }
	    int len = strlen(line);
	    if (line[len-1] != '\n') {
		fprintf(stderr, "%s:%d: %s\n", keyfile, linenum, "key too long");
		return -1;
	    }
	    line[len-1] = '\0';
	    keys[n_keys++] = strdup(line);
	}
	if (ferror(f)) {
	    fprintf(stderr, "%s: %s\n", keyfile, strerror(errno));
	    return -1;
	}

	if (n_keys > 0) {
	    rc = dbats_bulk_get_key_id(handler, NULL, n_keys,
		(const char * const *)keys, keyids, DBATS_CREATE);
	    if (rc == 0)
		dbats_log(DBATS_LOG_INFO, "Initialized %d keys", n_keys);
	}
	fclose(f);
    }
    dbats_commit_open(handler); // commit the txn started by dbats_open

    elapsed = time(NULL) - run_start;

    dbats_log(DBATS_LOG_INFO, "Time elapsed: %u sec", elapsed);
    dbats_log(DBATS_LOG_INFO, "Closing %s", dbats_path);
    if (dbats_close(handler) != 0)
	return -1;
    dbats_log(DBATS_LOG_INFO, "Done");
    return rc;
}
