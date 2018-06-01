#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "uint.h"
#include <db.h>
#include "dbats.h"

static char *progname = 0;

/* *********************************** */

static void help(void) {
    fprintf(stderr, "%s [{options}] {dbats_path}\n", progname);
    fprintf(stderr, "input options:\n");
    fprintf(stderr, "-a{N}        aggregation bundle ID (default: 0)\n");
    fprintf(stderr, "-v{N}        verbosity level\n");
    fprintf(stderr, "-x           obtain exclusive lock on db\n");
    fprintf(stderr, "-t           don't use transactions (fast, but unsafe)\n");
    fprintf(stderr, "-k{path}     load list of keys from {path} (default: use all keys in db)\n");
    fprintf(stderr, "-b{begin}    begin time (default: first time in db)\n");
    fprintf(stderr, "-e{end}      end time (default: last time in db)\n");
    fprintf(stderr, "output options [optional]:\n");
    fprintf(stderr, "-O           dbats output path (must exist)\n");
    fprintf(stderr, "-X           obtain exclusive lock on output db\n");
    fprintf(stderr, "-T           don't use transactions while writing (fast, but unsafe)\n");
    fprintf(stderr, "-P           preload timeslices\n");
    fprintf(stderr, "-U           allow updates to existing data\n");
    fprintf(stderr, "-Z           don't compress db\n");
    exit(-1);
}

/* ***************************************************************** */

#define MAX_KEYS 10000000
struct keyinfo {
    uint32_t keyid;
    char *key;
};
static struct keyinfo keys[MAX_KEYS];
static uint32_t n_keys = 0;

char *out_keys[MAX_KEYS];
uint32_t out_keyids[MAX_KEYS];

static void load_keys(dbats_handler *in_handler, dbats_handler *out_handler,
                      const char *path)
{
    FILE *keyfile = fopen(path, "r");
    if (!keyfile) {
	fprintf(stderr, "%s: %s\n", path, strerror(errno));
	exit(-1);
    }
    char keybuf[DBATS_KEYLEN+1];
    int rc;
    while (fgets(keybuf, sizeof(keybuf), keyfile)) {
	char *p = strchr(keybuf, '\n');
	if (p) *p = 0;
	if (dbats_get_key_id(in_handler, NULL, keybuf, &keys[n_keys].keyid, 0) != 0) {
	    fprintf(stderr, "no such key: %s\n", keybuf);
	    exit(-1);
	}
        out_keys[n_keys] = keys[n_keys].key = strdup(keybuf);
	n_keys++;
        if (n_keys >= MAX_KEYS) {
          dbats_log(DBATS_LOG_ERR,
                    "dbats_dump supports dumping at most %d keys", MAX_KEYS);
          exit (-1);
        }
    }
    if (out_handler) {
        dbats_log(DBATS_LOG_INFO, "dbats_bulk_get_key_id: %d keys", n_keys);
	rc = dbats_bulk_get_key_id(out_handler, NULL, &n_keys,
                                   (const char * const *)out_keys, out_keyids,
                                   DBATS_CREATE);
        if (rc != 0) {
            dbats_log(DBATS_LOG_ERR, "dbats_bulk_get_key_id failed");
            exit(-1);
        }
    }
    if (ferror(keyfile)) {
	fprintf(stderr, "%s: %s\n", path, strerror(errno));
	exit(-1);
    }
}

static void get_keys(dbats_handler *in_handler, dbats_handler *out_handler)
{
    dbats_keyid_iterator *dki;
    dbats_walk_keyid_start(in_handler, NULL, &dki);
    char keybuf[DBATS_KEYLEN];
    int rc;
    while (dbats_walk_keyid_next(dki, &keys[n_keys].keyid, keybuf) == 0) {
        out_keys[n_keys] = keys[n_keys].key = strdup(keybuf);
	n_keys++;
        if (n_keys >= MAX_KEYS) {
          dbats_log(DBATS_LOG_ERR,
                    "dbats_dump supports dumping at most %d keys", MAX_KEYS);
          exit (-1);
        }
    }
    if (out_handler) {
        dbats_log(DBATS_LOG_INFO, "dbats_bulk_get_key_id: %d keys", n_keys);
	rc = dbats_bulk_get_key_id(out_handler, NULL, &n_keys,
                                   (const char * const *)out_keys, out_keyids,
                                   DBATS_CREATE);
        if (rc != 0) {
            dbats_log(DBATS_LOG_ERR, "dbats_bulk_get_key_id failed");
            exit(-1);
        }
    }
    dbats_walk_keyid_end(dki);
}

int main(int argc, char *argv[]) {
    dbats_handler *in_handler;
    dbats_handler *out_handler = NULL;
    uint32_t opt_begin = 0, opt_end = 0;
    uint32_t run_start, elapsed;
    char *dbats_in_path = NULL;
    char *dbats_out_path = NULL;
    char *keyfile_path = NULL;
    FILE *out;
    progname = argv[0];
    int in_open_flags = DBATS_READONLY;
    int out_open_flags = 0;
    int out_select_flags = 0;
    int bundle_id = 0;

    int c;
    while ((c = getopt(argc, argv, "k:b:e:a:xtO:XTUPZv:")) != -1) {
	switch (c) {
            // input options
        case 'k':
	    keyfile_path = strdup(optarg);
	    break;
	case 'b':
	    opt_begin = atol(optarg);
	    break;
	case 'e':
	    opt_end = atol(optarg);
	    break;
        case 'a':
            bundle_id = atoi(optarg);
            break;
        case 'x':
	    in_open_flags |= DBATS_EXCLUSIVE;
	    break;
	case 't':
	    in_open_flags |= DBATS_NO_TXN;
	    break;

            // output options
        case 'O':
            dbats_out_path = strdup(optarg);
            break;
        case 'X':
	    out_open_flags |= DBATS_EXCLUSIVE;
	    break;
	case 'T':
	    out_open_flags |= DBATS_NO_TXN;
	    break;
        case 'U':
	    out_open_flags |= DBATS_UPDATABLE;
	    break;
        case 'P':
	    out_select_flags |= DBATS_PRELOAD;
	    break;
	case 'Z':
	    out_open_flags |= DBATS_UNCOMPRESSED;
	    break;

            // general options
        case 'v':
	    dbats_log_level = atoi(optarg);
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

    dbats_in_path = argv[0];

    dbats_log(DBATS_LOG_INFO, "begin=%"PRId32 " end=%"PRId32, opt_begin, opt_end);
    if (opt_end && opt_end < opt_begin)
	help();

    dbats_log(DBATS_LOG_INFO, "[in] opening %s", dbats_in_path);

    dbats_catch_signals();
    if (dbats_open(&in_handler, dbats_in_path, 0, 0, in_open_flags, 0) != 0)
	return(-1);

    const dbats_config *cfg = dbats_get_config(in_handler);

    dbats_commit_open(in_handler); // commit the txn started by dbats_open

    if (dbats_out_path) {
        dbats_log(DBATS_LOG_INFO, "[out] opening %s", dbats_out_path);
        if (dbats_open(&out_handler, dbats_out_path,
                       0, 0, out_open_flags, 0) != 0)
            return(-1);
        dbats_commit_open(out_handler); // commit the txn started by dbats_open
    }

    if (keyfile_path)
	load_keys(in_handler, out_handler, keyfile_path);
    else
	get_keys(in_handler, out_handler);

    tzset();
    out = stdout;
    run_start = time(NULL);

    uint32_t end = opt_end;
    if (end == 0) {
	dbats_get_end_time(in_handler, NULL, 0, &end);
	if (end == 0) {
	    dbats_log(DBATS_LOG_INFO, "[in] No data");
	    exit(0);
	}
    }

    if (bundle_id < 0 || bundle_id >= cfg->num_bundles) {
        dbats_log(DBATS_LOG_INFO,
                  "[in] Invalid bundle ID (%d) (min: %d, max: %d)\n",
                  bundle_id, 0, cfg->num_bundles);
        exit(0);
    }

    const dbats_bundle_info *bundle = dbats_get_bundle_info(in_handler, bundle_id);
    uint32_t t;

    uint32_t begin = opt_begin;
    if (begin == 0)
        dbats_get_start_time(in_handler, NULL, bundle_id, &begin);

    for (t = begin; t <= end && !dbats_caught_signal; t += bundle->period) {
        int rc;
        dbats_snapshot *in_snapshot;
        dbats_snapshot *out_snapshot = NULL;

        if ((rc = dbats_select_snap(in_handler, &in_snapshot, t, 0)) != 0) {
            dbats_log(DBATS_LOG_INFO, "[in] unable to find time %u", t);
            continue;
        }

        if (out_handler) {
retry:
            if (dbats_caught_signal) return EINTR;
            dbats_log(DBATS_LOG_INFO, "[out] select time %u", t);
            rc = dbats_select_snap(out_handler, &out_snapshot, t, out_select_flags);
            if (rc != 0) {
                dbats_log(DBATS_LOG_ERR, "[out] error in dbats_select_snap()");
                return -1;
            }
        }

        const dbats_value *values;
        for (int k = 0; k < n_keys && !dbats_caught_signal; k++) {
            rc = dbats_get(in_snapshot, keys[k].keyid, &values, bundle_id);
            if (rc == DB_NOTFOUND) {
                continue;
            }
            if (rc != 0) {
                fprintf(stderr, "[in] error in dbats_get(%s): rc=%d\n",
                        keys[k].key, rc);
                dbats_abort_snap(in_snapshot);
                break;
            }

            if (out_handler) {
                if (dbats_caught_signal) {
                    dbats_abort_snap(out_snapshot);
                    return EINTR;
                }
                rc = dbats_set(out_snapshot, keys[k].keyid, values);
                if (rc != 0) {
                    dbats_abort_snap(out_snapshot);
                    if (rc == DB_LOCK_DEADLOCK) {
                        dbats_log(DBATS_LOG_WARN,
                                  "[out] deadlock in dbats_set()");
                        goto retry;
                    }
                    dbats_log(DBATS_LOG_ERR, "[out] error in dbats_set(): %s",
                              db_strerror(rc));
                    return -1;
                }
            } else {
                fprintf(out, "%s ", keys[k].key);
                if (bundle->func == DBATS_AGG_AVG) {
                    for (int j = 0; j < cfg->values_per_entry; j++)
                        fprintf(out, "%.3f ", values ? values[j].d : 0);
                } else {
                    for (int j = 0; j < cfg->values_per_entry; j++)
                        fprintf(out, "%" PRIu64 " ", values ? values[j].u64 : 0);
                }
                fprintf(out, "%u\n", t);
            }
	}
        dbats_commit_snap(in_snapshot);
        if (out_snapshot && (dbats_commit_snap(out_snapshot) != 0)) {
            if (rc == DB_LOCK_DEADLOCK) {
                dbats_log(DBATS_LOG_WARN, "[out] deadlock in dbats_commit()");
                goto retry;
            }
            dbats_log(DBATS_LOG_ERR, "[out] error in dbats_commit(): %s",
                      db_strerror(rc));
            return -1;
        }
    }

    elapsed = time(NULL) - run_start;

    dbats_log(DBATS_LOG_INFO, "Time elapsed: %u sec", elapsed);
    dbats_log(DBATS_LOG_INFO, "[in] closing %s", dbats_in_path);
    if (dbats_close(in_handler) != 0)
	return -1;
    if (out_handler) {
        dbats_log(DBATS_LOG_INFO, "[out] closing %s", dbats_out_path);
        if (dbats_close(out_handler) != 0)
            return -1;
    }
    dbats_deliver_signal(); // if signal was caught, exit as if it was uncaught
    dbats_log(DBATS_LOG_INFO, "Done");
    return 0;
}
