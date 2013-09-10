#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dbats.h"

static char *progname = 0;

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path}\n", progname);
    fprintf(stderr, "Display information about a DBATS database.\n");
    fprintf(stderr, "options:\n");
    fprintf(stderr, "-v{N}    verbosity level\n");
    fprintf(stderr, "-x       obtain exclusive lock on db\n");
    fprintf(stderr, "-t       don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-k       also print list of keys in db\n");
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
    printf("%s%ld (%s)\n", label, t, buf);
}

int main(int argc, char *argv[]) {
    char *dbats_path;
    dbats_handler *handler;
    uint32_t period = 60;
    int open_flags = DBATS_READONLY;
    int opt_keys = 0;
    progname = argv[0];
    int rc = 0;
    const dbats_config *cfg;

    dbats_log_level = LOG_INFO;

    int c;
    while ((c = getopt(argc, argv, "v:xtk")) != -1) {
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
	    opt_keys = 1;
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
    dbats_commit(handler);

    cfg = dbats_get_config(handler);
    printf("version: %d\n", cfg->version);
    printf("values_per_entry: %d\n", cfg->values_per_entry);
    printf("entry_size: %d bytes\n", cfg->entry_size);
    uint32_t num_keys;
    dbats_num_keys(handler, &num_keys);
    printf("keys: %d\n", num_keys);
    print_duration("period: ", cfg->period);

    for (int bid = 0; bid < cfg->num_bundles; bid++) {
	const dbats_bundle_info *bundle = dbats_get_bundle_info(handler, bid);
	uint32_t start, end;
	dbats_get_start_time(handler, bid, &start);
	dbats_get_end_time(handler, bid, &end);
	printf("bundle %d:\n", bid);
	printf("  function: %s\n", dbats_agg_func_label[bundle->func]);
	printf("  steps: %u\n", bundle->steps);
	print_duration("  period: ", bundle->period);
	printf("  keep: %u\n", bundle->keep);
	print_time("  start: ", start);
	print_time("  end:   ", end);
    }

    if (opt_keys) {
	printf("keys:\n");
	dbats_walk_keyid_start(handler);
	uint32_t keyid;
	char keybuf[DBATS_KEYLEN];
	while (dbats_walk_keyid_next(handler, &keyid, keybuf) == 0) {
	    printf("  %8u: %s\n", keyid, keybuf);
	}
	dbats_walk_keyid_end(handler);
    }

    dbats_close(handler);

    return rc;
}