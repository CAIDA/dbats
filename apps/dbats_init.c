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
    fprintf(stderr, "%s [{options}] {dbats_path} [{func}:{steps}]...\n",
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
		dbats_log(DBATS_LOG_ERR, "error: illegal -p value: %s", optarg);
		help();
	    }
	    break;
	case 'e':
	    values_per_entry = atoi(optarg);
	    if (values_per_entry <= 0) {
		dbats_log(DBATS_LOG_ERR, "error: illegal -e value: %s", optarg);
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
	dbats_log(DBATS_LOG_ERR, "error: missing dbats_path");
	help();
    }
    dbats_path = argv[0];
    argv++;
    argc--;

    char funcname[10];
    char garbage[2];
    enum dbats_agg_func *aggfunc = malloc(argc * sizeof(enum dbats_agg_func));
    int *aggsteps = malloc(argc * sizeof(int));
    for (int i = 0; i < argc; i++) {
	int n = sscanf(argv[i], "%9[A-Za-z0-9_]:%d%1s",
	    funcname, &aggsteps[i], garbage);
	if (n != 2) {
	    dbats_log(DBATS_LOG_ERR, "argv[%d]: \"%s\": n=%d", i, argv[i], n);
	    help();
	}
	aggfunc[i] = dbats_find_agg_func(funcname);
	if (aggfunc[i] == DBATS_AGG_NONE) {
	    dbats_log(DBATS_LOG_ERR, "no such function: %s", funcname);
	    return -1;
	}
    }

    run_start = time(NULL);
    dbats_log(DBATS_LOG_INFO, "%s: open", progname);
    if (dbats_open(&handler, dbats_path, values_per_entry, period, open_flags, 0644) != 0)
	return -1;

    const dbats_config *cfg = dbats_get_config(handler);

    for (int i = 0; i < argc; i++) {
	int skip = 0;
	for (int bid = 0; bid < cfg->num_bundles; bid++) {
	    const dbats_bundle_info *bundle =
		dbats_get_bundle_info(handler, bid);
	    if (bundle->func == aggfunc[i] && bundle->steps == aggsteps[i]) {
		dbats_log(DBATS_LOG_INFO, "skipping duplicate aggregate %s:%d",
		    dbats_agg_func_label[bundle->func], bundle->steps);
		goto skip;
	    }
	}
	if (skip) continue;
	if ((dbats_aggregate(handler, aggfunc[i], aggsteps[i])) != 0)
	    return -1;
	skip: ;
    }

    if (keyfile) {
	FILE *f = fopen(keyfile, "r");
	if (!f) {
	    dbats_log(DBATS_LOG_ERR, "%s: %s", keyfile, strerror(errno));
	    return -1;
	}
	char line[DBATS_KEYLEN+1];
	int linenum = 0;
	while (fgets(line, sizeof(line), f)) {
	    linenum++;
	    if (n_keys >= MAX_KEYS) {
		dbats_log(DBATS_LOG_ERR, "%s:%d: %s", keyfile, linenum,
		    "too many keys");
		return -1;
	    }
	    int len = strlen(line);
	    if (line[len-1] != '\n') {
		dbats_log(DBATS_LOG_ERR, "%s:%d: %s", keyfile, linenum,
		    "key too long");
		return -1;
	    }
	    line[len-1] = '\0';
	    keys[n_keys++] = strdup(line);
	}
	if (ferror(f)) {
	    dbats_log(DBATS_LOG_ERR, "%s: %s", keyfile, strerror(errno));
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
