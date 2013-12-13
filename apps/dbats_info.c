#define _POSIX_C_SOURCE 200809L
#include <unistd.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "uint.h"
#include <db.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path}\n", progname);
    fprintf(stderr, "Display information about a DBATS database.\n");
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}       verbosity level\n");
    fprintf(stderr, "-x          obtain exclusive lock on db\n");
    fprintf(stderr, "-t          don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-s          print summary statistics\n");
    fprintf(stderr, "-k          print list of all keys in db, ordered by id\n");
    fprintf(stderr, "-K{pattern} print list of keys in db matching {pattern}\n");
    fprintf(stderr, "-K ''       print list of all keys in db, ordered by name\n");
    fprintf(stderr, "-a          print all information (-s -k)\n");
    exit(-1);
}

static void print_duration(const char *label, uint32_t t)
{
    char buf[64] = "";
    if (t % 86400 == 0)
	sprintf(buf, " (%d days)", t / 86400);
    else if (t % 3600 == 0)
	sprintf(buf, " (%d hours)", t / 3600);
    else if (t % 60 == 0)
	sprintf(buf, " (%d min)", t / 60);
    printf("%s%us%s\n", label, t, buf);
}

static void print_time(const char *label, time_t t)
{
    char buf[64];
    struct tm *tm;
    tm = gmtime(&t);
    strftime(buf, sizeof(buf), "%F %T UTC", tm);
    printf("%s%" PRId64 " (%s)\n", label, (int64_t)t, buf);
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    int open_flags = DBATS_READONLY;
    int opt_summary = 0;
    int opt_keys = 0;
    char *keyglob = NULL;
    progname = argv[0];
    int rc = 0;

    int c;
    while ((c = getopt(argc, argv, "v:xtaskK:")) != -1) {
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
	case 'a':
	    opt_summary = 1;
	    opt_keys = 1;
	    break;
	case 's':
	    opt_summary = 1;
	    break;
	case 'k':
	    opt_keys = 1;
	    break;
	case 'K':
	    keyglob = strdup(optarg);
	    break;
	default:
	    help();
	    break;
	}
    }
    argv += optind;
    argc -= optind;

    if (optind == 1) // no options given
	opt_summary = 1;

    if (argc != 1)
	help();
    dbats_path = argv[0];

    dbats_catch_signals();
    if (dbats_open(&handler, dbats_path, 1, 0, open_flags, 0) != 0)
	return -1;
    dbats_commit_open(handler);

    if (opt_summary) {
	const dbats_config *cfg = dbats_get_config(handler);
	printf("dbats database version: %d\n", cfg->version);
	printf("values_per_entry: %d\n", cfg->values_per_entry);
	printf("entry_size: %d bytes\n", cfg->entry_size);
	uint32_t num_keys;
	dbats_num_keys(handler, &num_keys);
	printf("keys: %d\n", num_keys);
	print_duration("period: ", cfg->period);

	for (int bid = 0; bid < cfg->num_bundles && !dbats_caught_signal; bid++) {
	    const dbats_bundle_info *bundle =
		dbats_get_bundle_info(handler, bid);
	    uint32_t start, end;
	    dbats_get_start_time(handler, NULL, bid, &start);
	    dbats_get_end_time(handler, NULL, bid, &end);
	    printf("bundle %d:\n", bid);
	    printf("  function: %s\n", dbats_agg_func_label[bundle->func]);
	    printf("  steps: %u\n", bundle->steps);
	    print_duration("  period: ", bundle->period);
	    printf("  keep: %u\n", bundle->keep);
	    print_time("  start: ", start);
	    print_time("  end:   ", end);
	}
    }

    if (keyglob && !dbats_caught_signal) {
	if (!*keyglob) {
	    keyglob = NULL;
	    printf("all keys (by name):\n");
	} else {
	    printf("keys matching \"%s\":\n", keyglob);
	}
	dbats_keytree_iterator *dki;
	if (dbats_glob_keyname_start(handler, NULL, &dki, keyglob) == 0) {
	    uint32_t keyid;
	    char keybuf[DBATS_KEYLEN];
	    while ((rc = dbats_glob_keyname_next(dki, &keyid, keybuf)) == 0) {
		printf("  %10u: %s\n", keyid, keybuf);
		if (dbats_caught_signal) break;
	    }
	    if (rc == 0 || rc == DB_NOTFOUND) {
		rc = 0;
	    } else {
		dbats_log(DBATS_LOG_ERR,
		    "internal error in dbats_glob_keyname_next: %s",
		    db_strerror(rc));
	    }
	    dbats_glob_keyname_end(dki);
	}
    }

    if (opt_keys && !dbats_caught_signal) {
	printf("all keys (by id):\n");
	dbats_keyid_iterator *dki;
	if (dbats_walk_keyid_start(handler, NULL, &dki) == 0) {
	    uint32_t keyid;
	    char keybuf[DBATS_KEYLEN];
	    while ((rc = dbats_walk_keyid_next(dki, &keyid, keybuf)) == 0) {
		printf("  %10u: %s\n", keyid, keybuf);
		if (dbats_caught_signal) break;
	    }
	    if (rc == 0 || rc == DB_NOTFOUND) {
		rc = 0;
	    } else {
		dbats_log(DBATS_LOG_ERR,
		    "internal error in dbats_walk_keyid_next: %s",
		    db_strerror(rc));
	    }
	    dbats_walk_keyid_end(dki);
	}
    }

    dbats_close(handler);

    dbats_deliver_signal(); // if a signal was caught, exit as if it weren't
    return rc ? 1 : 0;
}
