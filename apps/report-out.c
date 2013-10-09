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
    fprintf(stderr, "-v{N}        verbosity level\n");
    fprintf(stderr, "-x           obtain exclusive lock on db\n");
    fprintf(stderr, "-t           don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-k{path}     load list of keys from {path} (default: use all keys in db)\n");
    fprintf(stderr, "-b{begin}    begin time (default: first time in db)\n");
    fprintf(stderr, "-e{end}      end time (default: last time in db)\n");
    fprintf(stderr, "-o text      output text (default)\n");
    fprintf(stderr, "-o gnuplot   output gnuplot script\n");
    exit(-1);
}

/* ***************************************************************** */

#define MAX_KEYS 10000000
struct keyinfo {
    uint32_t keyid;
    char *key;
};
static struct keyinfo keys[MAX_KEYS];
static int n_keys = 0;

static void load_keys(dbats_handler *handler, const char *path)
{
    FILE *keyfile = fopen(path, "r");
    if (!keyfile) {
	fprintf(stderr, "%s: %s\n", path, strerror(errno));
	exit(-1);
    }
    char keybuf[DBATS_KEYLEN+1];
    while (fgets(keybuf, sizeof(keybuf), keyfile)) {
	char *p = strchr(keybuf, '\n');
	if (p) *p = 0;
	if (dbats_get_key_id(handler, NULL, keybuf, &keys[n_keys].keyid, 0) != 0) {
	    fprintf(stderr, "no such key: %s\n", keybuf);
	    exit(-1);
	}
	keys[n_keys].key = strdup(keybuf);
	n_keys++;
    }
    if (ferror(keyfile)) {
	fprintf(stderr, "%s: %s\n", path, strerror(errno));
	exit(-1);
    }
}

static void get_keys(dbats_handler *handler)
{
    dbats_keyid_iterator *dki;
    dbats_walk_keyid_start(handler, NULL, &dki);
    char keybuf[DBATS_KEYLEN];
    while (dbats_walk_keyid_next(dki, &keys[n_keys].keyid, keybuf) == 0) {
	keys[n_keys].key = strdup(keybuf);
	n_keys++;
    }
    dbats_walk_keyid_end(dki);
}

enum { OT_TEXT, OT_GNUPLOT };

int main(int argc, char *argv[]) {
    dbats_handler *handler;
    uint32_t opt_begin = 0, opt_end = 0;
    uint32_t run_start, elapsed;
    char *dbats_path = NULL;
    char *keyfile_path = NULL;
    FILE *out;
    progname = argv[0];
    int outtype = OT_TEXT;
    int open_flags = DBATS_READONLY;
    uint32_t bundle0_period;

    int c;
    while ((c = getopt(argc, argv, "v:xtk:b:e:o:")) != -1) {
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
	case 'k':
	    keyfile_path = strdup(optarg);
	    break;
	case 'b':
	    opt_begin = atol(optarg);
	    break;
	case 'e':
	    opt_end = atol(optarg);
	    break;
	case 'o':
	    if (strcmp(optarg, "text") == 0) {
		outtype = OT_TEXT;
	    } else if (strcmp(optarg, "gnuplot") == 0) {
		outtype = OT_GNUPLOT;
	    } else {
		fprintf(stderr, "unknown output type \"%s\"\n", optarg);
		help();
	    }
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

    dbats_log(DBATS_LOG_INFO, "begin=%"PRId32 " end=%"PRId32, opt_begin, opt_end);
    if (opt_end < opt_begin)
	help();

    dbats_log(DBATS_LOG_INFO, "Opening %s", dbats_path);

    if (dbats_open(&handler, dbats_path, 0, 0, open_flags) != 0)
	return(-1);

    const dbats_config *cfg = dbats_get_config(handler);

    const dbats_bundle_info *bundle = dbats_get_bundle_info(handler, 0);
    bundle0_period = bundle->period;

    dbats_commit_open(handler); // commit the txn started by dbats_open

    if (keyfile_path)
	load_keys(handler, keyfile_path);
    else
	get_keys(handler);

    tzset();
    out = stdout;
    run_start = time(NULL);

    uint32_t end = opt_end;
    if (end == 0) {
	dbats_get_end_time(handler, NULL, 0, &end);
	if (end == 0) {
	    dbats_log(DBATS_LOG_INFO, "No data");
	    exit(0);
	}
    }

    if (outtype == OT_GNUPLOT) {
	uint32_t begin = opt_begin;
	if (begin == 0) {
	    // find earliest start time of all bundles
	    dbats_get_start_time(handler, NULL, 0, &begin);
	    for (int bid = 1; bid < cfg->num_bundles; bid++) {
		uint32_t bundle_begin;
		dbats_get_start_time(handler, NULL, bid, &bundle_begin);
		bundle = dbats_get_bundle_info(handler, bid);
		if (begin > bundle_begin)
		    begin = bundle_begin;
	    }
	}

	fprintf(out, "set style data boxes\n");
	fprintf(out, "set style fill empty\n");
	fprintf(out, "set xrange [%" PRIu32 ":%" PRIu32 "]\n",
	    0, end + bundle0_period - begin);
	fprintf(out, "set yrange [0:*]\n");
	fprintf(out, "set key bottom left\n");
	const dbats_bundle_info *bundle1;
	if (cfg->num_bundles > 0) {
	    bundle1 = dbats_get_bundle_info(handler, 1);
	    fprintf(out, "set xtics %d\n", bundle1->period);
	    fprintf(out, "set mxtics %d\n", bundle1->steps);
	    fprintf(out, "set grid xtics\n");
	}
	const char *prefix = "plot";
	for (int bid = 0; bid < cfg->num_bundles; bid++) {
	    bundle = dbats_get_bundle_info(handler, bid);
	    fprintf(out, "%s '-' using ($1-%"PRIu32"+%f):($2):(%d) %s"
		"linecolor %d title \"%"PRIu32"s %s\"",
		prefix, begin, bundle->period/2.0, bundle->period,
		bid == 0 ? "with boxes fs solid 0.1 " : "",
		bid, bundle->period, dbats_agg_func_label[bundle->func]);
	    prefix = ", \\\n    ";
	}
	fprintf(out, "\n");
    }

    for (int bid = 0; bid < cfg->num_bundles; bid++) {
	bundle = dbats_get_bundle_info(handler, bid);
	uint32_t t;

	uint32_t begin = opt_begin;
	if (begin == 0)
	    dbats_get_start_time(handler, NULL, bid, &begin);

	for (t = begin; t <= end; t += bundle->period) {
	    int rc;
	    dbats_snapshot *snapshot;

	    if ((rc = dbats_select_snap(handler, &snapshot, t, 0)) != 0) {
		dbats_log(DBATS_LOG_INFO, "Unable to find time %u", t);
		continue;
	    }

	    if (bundle->func == DBATS_AGG_AVG) {
		const double *values;
		for (int k = 0; k < n_keys; k++) {
		    rc = dbats_get_double(snapshot, keys[k].keyid, &values, bid);
		    if (rc == DB_NOTFOUND)
			continue;
		    if (rc != 0) {
			fprintf(stderr, "error in dbats_get(%s): rc=%d\n", keys[k].key, rc);
			dbats_abort_snap(snapshot);
			break;
		    }
		    switch (outtype) {
		    case OT_TEXT:
			fprintf(out, "%s ", keys[k].key);
			for (int j = 0; j < cfg->values_per_entry; j++) {
			    fprintf(out, "%.3f ", values ? values[j] : 0);
			}
			fprintf(out, "%u %d\n", t, bid);
			break;
		    case OT_GNUPLOT:
			fprintf(out, "%u %.3f\n",
			    t, values ? values[0] : 0);
			break;
		    }
		}
	    } else {
		const dbats_value *values;
		for (int k = 0; k < n_keys; k++) {
		    rc = dbats_get(snapshot, keys[k].keyid, &values, bid);
		    if (rc == DB_NOTFOUND)
			continue;
		    if (rc != 0) {
			fprintf(stderr, "error in dbats_get(%s): rc=%d\n", keys[k].key, rc);
			dbats_abort_snap(snapshot);
			break;
		    }
		    switch (outtype) {
		    case OT_TEXT:
			fprintf(out, "%s ", keys[k].key);
			for (int j = 0; j < cfg->values_per_entry; j++) {
			    fprintf(out, "%" PRIval " ", values ? values[j] : 0);
			}
			fprintf(out, "%u %d\n", t, bid);
			break;
		    case OT_GNUPLOT:
			fprintf(out, "%u %" PRIval "\n",
			    t, values ? values[0] : 0);
			break;
		    }
		}
	    }
	    dbats_commit_snap(snapshot);
	}
	if (outtype == OT_GNUPLOT) {
	    fprintf(out, "e\n");
	}
    }

    elapsed = time(NULL) - run_start;

    dbats_log(DBATS_LOG_INFO, "Time elapsed: %u sec", elapsed);
    dbats_log(DBATS_LOG_INFO, "Closing %s", dbats_path);
    if (dbats_close(handler) != 0)
	return -1;
    dbats_log(DBATS_LOG_INFO, "Done");
    return 0;
}
