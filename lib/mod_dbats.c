/*
 * DBATS module for Apache 2.2 web server
 */

#define _POSIX_C_SOURCE 200809L
#include "uint.h"
#include <db.h>
#include "httpd.h"
#include "http_config.h"
#include "http_protocol.h"
#include "http_log.h"
#include "ap_config.h"
#include "apr_strings.h"
#include "apr_hash.h"
#include "dbats.h"

typedef struct {
    const char *path;
} mod_dbats_dir_config;

typedef struct {
    request_rec *r;
    unsigned long id;
    apr_table_t *queryparams;
    dbats_handler *dh;
    dbats_snapshot *snap;
    dbats_keytree_iterator *dki;
    int dbats_status;
    int http_status;
    int retries;
} mod_dbats_reqstate;

struct {
    apr_pool_t *pool;
    apr_thread_mutex_t *dht_mutex;
    apr_hash_t *dht; // hash table of dbats_path -> dbats_handler
    apr_thread_mutex_t *reqcount_mutex;
    unsigned long reqcount;
} procstate;

module AP_MODULE_DECLARE_DATA dbats_module;

apr_threadkey_t *tlskey_req = NULL; // thread-local storage key for request
server_rec *main_server = NULL;

#define log_rerror(level, reqstate, fmt, ...) \
    ap_log_rerror(APLOG_MARK, level, 0, reqstate->r, "%u#%lu: " fmt, getpid(), reqstate->id, __VA_ARGS__)

static const char *set_path(cmd_parms *cmd, void *vcfg, const char *arg)
{
    mod_dbats_dir_config *cfg = (mod_dbats_dir_config*)vcfg;
    cfg->path = arg;
    return NULL;
}

static void log_callback(int level, const char *file, int line, const char *msg)
{
    void *vr;
    apr_threadkey_private_get(&vr, tlskey_req);
    request_rec *r = vr;

    level = (level >= DBATS_LOG_FINE) ? APLOG_DEBUG :
	(level >= DBATS_LOG_CONFIG) ? APLOG_INFO :
	(level >= DBATS_LOG_INFO) ? APLOG_NOTICE :
	(level >= DBATS_LOG_WARN) ? APLOG_WARNING :
	APLOG_ERR;
    if (r) {
	mod_dbats_reqstate *reqstate = (mod_dbats_reqstate*)ap_get_module_config(r->request_config, &dbats_module);
	ap_log_rerror(APLOG_MARK, level, 0, reqstate->r, "%u#%lu: " "%s", getpid(), reqstate->id, msg);
    } else { // e.g., during init or cleanup
	ap_log_error(APLOG_MARK, level, 0, main_server, "%s", msg);
    }
}

static apr_status_t mod_dbats_close(void *data)
{
    dbats_handler *dh = (dbats_handler*)data;
    ap_log_perror(APLOG_MARK, APLOG_DEBUG, 0, procstate.pool, "mod_dbats: mod_dbats_close pid=%u", getpid());
    int rc = dbats_close(dh);
    ap_log_perror(APLOG_MARK, APLOG_DEBUG, 0, procstate.pool, "mod_dbats: dbats_close: %d %s", rc, db_strerror(rc));
    return APR_SUCCESS;
}

static apr_status_t mod_dbats_threadkey_private_delete(void *data)
{
    return apr_threadkey_private_delete(data);
}

static void child_init_handler(apr_pool_t *pchild, server_rec *s)
{
    ap_log_error(APLOG_MARK, APLOG_INFO, 0, s, "mod_dbats on %s:%u: child_init_handler pid=%u", s->server_hostname, s->port, getpid());

    apr_thread_mutex_create(&procstate.reqcount_mutex, APR_THREAD_MUTEX_DEFAULT, pchild);

    procstate.pool = pchild;
    procstate.dht = apr_hash_make(pchild);

    apr_thread_mutex_create(&procstate.dht_mutex, APR_THREAD_MUTEX_DEFAULT, pchild);

    main_server = s;
    apr_threadkey_private_create(&tlskey_req, NULL, pchild);
    apr_pool_cleanup_register(pchild, tlskey_req, mod_dbats_threadkey_private_delete, NULL);

    dbats_log_callback = log_callback;
}

// Parse URL query parameters into a table.  Each entry is a list of values.
static void args_to_table(request_rec *r, apr_table_t **tablep)
{
    *tablep = apr_table_make(r->pool, 0);
    if (!r->args) return;
    char *name, *value, *next;
    size_t nlen, vlen;
    for (name = apr_pstrdup(r->pool, r->args); name; name = next) {
	nlen = strcspn(name, "&=");
	next = name[nlen] ? name + nlen + 1 : NULL;
	if (name[nlen] == '=') {
	    value = next;
	    vlen = strcspn(value, "&");
	    next = value[vlen] ? value + vlen + 1 : NULL;
	    value[vlen] = '\0';
	    ap_unescape_url(value);
	} else {
	    value = name + nlen;
	}
	name[nlen] = '\0';
	ap_unescape_url(name);
	apr_array_header_t *plist = (apr_array_header_t*)apr_table_get(*tablep, name);
	if (!plist) {
	    plist = apr_array_make(r->pool, 1, sizeof(char*));
	    apr_table_addn(*tablep, name, (void*)plist);
	}
	ap_log_perror(APLOG_MARK, APLOG_DEBUG, 0, r->pool, "query param: %s[%d]=%s",
	    name, plist->nelts, value);
	APR_ARRAY_PUSH(plist, char*) = value;
    }
}

static inline char *get_first_param(apr_table_t *queryparams, const char *key)
{
    apr_array_header_t *plist = (apr_array_header_t*)apr_table_get(queryparams, key);
    return plist ? APR_ARRAY_IDX(plist, 0, char*) : NULL;
}

static void metrics_find(request_rec *r, mod_dbats_reqstate *reqstate)
{
    int rc;
    int i;
    uint32_t keyid;
    char name[DBATS_KEYLEN];

    typedef struct {
	const char *name;
	uint8_t isleaf;
    } mfinfo_t;

    const char *query = get_first_param(reqstate->queryparams, "query");
    const char *format = get_first_param(reqstate->queryparams, "format");
    //const char *logLevel = get_first_param(reqstate->queryparams, "logLevel");

    if (!query) {
	reqstate->http_status = HTTP_BAD_REQUEST;
	return;
    }

    log_rerror(APLOG_DEBUG, reqstate, "# query: %s", query);

    rc = dbats_glob_keyname_start(reqstate->dh, NULL, &reqstate->dki, query);
    if (rc != 0) {
	log_rerror(APLOG_DEBUG, reqstate, "mod_dbats: dbats_glob_keyname_start: %s", db_strerror(rc));
	reqstate->dbats_status = rc;
	reqstate->dki = NULL;
	return;
    }

    apr_array_header_t *metrics = apr_array_make(r->pool, 1, sizeof(mfinfo_t*));

    while ((rc = dbats_glob_keyname_next(reqstate->dki, &keyid, name)) == 0) {
	mfinfo_t *info = apr_palloc(r->pool, sizeof(mfinfo_t));
	info->name = apr_pstrdup(r->pool, name);
	info->isleaf = !(keyid & DBATS_KEY_IS_PREFIX);
	APR_ARRAY_PUSH(metrics, mfinfo_t*) = info;
    }

    dbats_glob_keyname_end(reqstate->dki);
    reqstate->dki = NULL;

    // DB_PAGE_NOTFOUND isn't possible according to docs, but it happens,
    // and _seems_ to be the same as DB_NOTFOUND, so we treat it as such.
    if (rc == DB_PAGE_NOTFOUND) {
	log_rerror(APLOG_NOTICE, reqstate,
	    "mod_dbats: warning: dbats_glob_keyname_next: %s", db_strerror(rc));
    } else if (rc != DB_NOTFOUND) {
	log_rerror(APLOG_NOTICE, reqstate,
	    "mod_dbats: dbats_glob_keyname_next: %s", db_strerror(rc));
	reqstate->dbats_status = rc;
	return;
    }

    if (format && strcmp(format, "html") == 0) {
	ap_set_content_type(r, "text/html");
	ap_rprintf(r, "<!DOCTYPE html>\n");
	ap_rprintf(r, "<html>\n");
	ap_rprintf(r, "<head>\n");
	ap_rprintf(r, "<title>DBATS key query</title>\n");
	ap_rprintf(r, "</head>\n");
	ap_rprintf(r, "<body>\n");
	ap_rprintf(r, "<h1>DBATS key query</h1>\n");
	ap_rprintf(r, "<ul>\n");
	for (i = 0; i < metrics->nelts; i++) {
	    mfinfo_t *info = APR_ARRAY_IDX(metrics, i, mfinfo_t*);
	    ap_rprintf(r, "<li>%s\n", ap_escape_html(r->pool, info->name));
	}
	ap_rprintf(r, "</ul>\n");
	ap_rprintf(r, "</body>\n");
	ap_rprintf(r, "</html>\n");

    } else if (format && strcmp(format, "json-internal") == 0) {
	uint32_t start, end, nstart, bstart;
	const dbats_config *dbcfg = dbats_get_config(reqstate->dh);
	int bid;
	dbats_get_end_time(reqstate->dh, NULL, 0, &end);
	dbats_get_start_time(reqstate->dh, NULL, 0, &start);
	for (bid = 1; bid < dbcfg->num_bundles; bid++) {
	    nstart = start;
	    dbats_normalize_time(reqstate->dh, bid, &nstart);
	    rc = dbats_get_start_time(reqstate->dh, NULL, 0, &bstart);
	    if (bstart < nstart)
		start = bstart;
	}
	ap_set_content_type(r, "application/json");
	int n = 0;
	ap_rprintf(r, "[");
	for (i = 0; i < metrics->nelts; i++) {
	    mfinfo_t *info = APR_ARRAY_IDX(metrics, i, mfinfo_t*);
	    const char *ename = ap_escape_quotes(r->pool, info->name);
	    ap_rprintf(r, "%s\n{", n ? "," : "");
	    ap_rprintf(r, "\"path\": \"%s\", ", ename);
	    ap_rprintf(r, "\"is_leaf\": %d, ", info->isleaf);
	    ap_rprintf(r, "\"intervals\": [{\"start\": %u, \"end\": %u}]}", start, end);
	    n++;
	}
	ap_rprintf(r, "\n]\n");

    } else { // default to json
	ap_set_content_type(r, "application/json");
	int n = 0;
	ap_rprintf(r, "[");
	for (i = 0; i < metrics->nelts; i++) {
	    mfinfo_t *info = APR_ARRAY_IDX(metrics, i, mfinfo_t*);
	    const char *ename = ap_escape_quotes(r->pool, info->name);
	    const char *lastpart = strrchr(ename, '.');
	    lastpart = lastpart ? lastpart + 1 : ename;
	    ap_rprintf(r, "%s\n{", n ? "," : "");
	    ap_rprintf(r, "\"text\": \"%s\", ", lastpart);
	    ap_rprintf(r, "\"expandable\": %d, ", !info->isleaf);
	    ap_rprintf(r, "\"leaf\": %d, ", info->isleaf);
	    ap_rprintf(r, "\"id\": \"%s\", ", ename);
	    ap_rprintf(r, "\"allowChildren\": %d}", !info->isleaf);
	    n++;
	}
	ap_rprintf(r, "\n]\n");
    }
}

static void metrics_index(request_rec *r, mod_dbats_reqstate *reqstate)
{
    int rc;
    uint32_t keyid;
    char name[DBATS_KEYLEN];
    uint32_t expected = -1;
    int i;

    rc = dbats_num_keys(reqstate->dh, &expected);
    if (rc != 0) {
	log_rerror(APLOG_NOTICE, reqstate, "mod_dbats: dbats_num_keys: %s", db_strerror(rc));
	reqstate->dbats_status = rc;
	return;
    }
    apr_array_header_t *keys = apr_array_make(r->pool, expected, sizeof(char*));

    rc = dbats_glob_keyname_start(reqstate->dh, NULL, &reqstate->dki, NULL);
    if (rc != 0) {
	log_rerror(APLOG_NOTICE, reqstate, "mod_dbats: dbats_glob_keyname_start: %s", db_strerror(rc));
	reqstate->dbats_status = rc;
	reqstate->dki = NULL;
	return;
    }
    while ((rc = dbats_glob_keyname_next(reqstate->dki, &keyid, name)) == 0) {
	APR_ARRAY_PUSH(keys, char*) = apr_pstrdup(r->pool, name);
    }
    dbats_glob_keyname_end(reqstate->dki);
    reqstate->dki = NULL;

    // It seems that rc sometimes contains DB_PAGE_NOTFOUND, which should be
    // impossible.  We abort only if we didn't get the expected number of keys.
    if (rc != DB_NOTFOUND) {
	if (keys->nelts != expected) {
	    log_rerror(APLOG_NOTICE, reqstate,
		"ERROR: dbats_glob_keyname_next: %d %s (got %d of %d keys)",
		rc, db_strerror(rc), keys->nelts, expected);
	    reqstate->dbats_status = rc;
	    return;
	}
	log_rerror(APLOG_NOTICE, reqstate,
	    "dbats_glob_keyname_next: %d %s (got all %d keys)",
	    rc, db_strerror(rc), expected);
    }

    ap_set_content_type(r, "application/json");
    ap_rprintf(r, "[");
    for (i = 0; i < keys->nelts; i++) {
	ap_rprintf(r, "%s\n    \"%s\"", i > 0 ? "," : "",
	    ap_escape_quotes(r->pool, APR_ARRAY_IDX(keys, i, char*)));
    }
    ap_rprintf(r, "\n]\n");
}

static void render(request_rec *r, mod_dbats_reqstate *reqstate)
{
    int rc;
    int bid = 0;
    int i;
    uint32_t q_from, from, q_until, until, max_points = 0;
    enum dbats_agg_func agg_func = DBATS_AGG_AVG;
    int no_pad = 0;
    int nsamples = 0;
    const dbats_bundle_info *bundle = NULL;

    apr_array_header_t *targets = (apr_array_header_t*)apr_table_get(reqstate->queryparams, "target");
    const char *str_from = get_first_param(reqstate->queryparams, "from");
    const char *str_until = get_first_param(reqstate->queryparams, "until");
    const char *str_max_points = get_first_param(reqstate->queryparams, "maxDataPoints");
    const char *str_agg_func = get_first_param(reqstate->queryparams, "aggFunc");
    const char *format = get_first_param(reqstate->queryparams, "format");
    const char *str_no_pad = get_first_param(reqstate->queryparams, "noPad");
    //const char *logLevel = get_first_param(reqstate->queryparams, "logLevel");

    reqstate->http_status = HTTP_BAD_REQUEST;
    if (!targets || !str_from || !str_until) return;
    char *p;
    from = strtol(str_from, &p, 10);
    if (!*str_from || *p) return;
    until = strtol(str_until, &p, 10);
    if (!*str_until || *p) return;
    if (str_max_points) {
	max_points = strtol(str_max_points, &p, 10);
	if (!*str_max_points || *p) return;
    }
    if (str_agg_func) {
	agg_func = dbats_find_agg_func(str_agg_func);
	if (agg_func == DBATS_AGG_NONE) return;
    }
    if (str_no_pad) {
        no_pad = strtol(str_no_pad, &p, 10);
        if (!*str_no_pad || *p) no_pad = 1; //noPad specified without arg
    }
    reqstate->http_status = OK;

    for (i = 0; i < targets->nelts; i++)
	log_rerror(APLOG_DEBUG, reqstate, "# target[%d]: %s", i, APR_ARRAY_IDX(targets, i, char*));

    log_rerror(APLOG_DEBUG, reqstate, "# from:   %" PRIu32, from);
    log_rerror(APLOG_DEBUG, reqstate, "# until:  %" PRIu32, until);
    log_rerror(APLOG_DEBUG, reqstate, "# max_points: %" PRIu32, max_points);

    bid = dbats_best_bundle(reqstate->dh, agg_func, from, until, max_points, 1);
    bundle = dbats_get_bundle_info(reqstate->dh, bid);
    dbats_normalize_time(reqstate->dh, bid, &from);
    dbats_normalize_time(reqstate->dh, bid, &until);
    q_from = from += bundle->period;
    q_until = until;

    uint32_t starttime, endtime;
    rc = dbats_get_start_time(reqstate->dh, NULL, bid, &starttime);
    if (rc == 0)
	rc = dbats_get_end_time(reqstate->dh, NULL, bid, &endtime);
    if (rc == 0) {
	if (from < starttime) from = starttime;
	if (until > endtime) until = endtime;
    } else if (rc == DB_NOTFOUND) {
	from = until = 0;
    } else {
	reqstate->http_status = HTTP_INTERNAL_SERVER_ERROR;
	return;
    }

    nsamples = (until < from) ? 0 : (until - from) / bundle->period + 1;

    typedef struct {
	const char *key;
	uint32_t keyid;
	dbats_value *data;
	uint8_t *isset;
    } chunk_t;

    // get keys
    char key[DBATS_KEYLEN];
    uint32_t keyid;

    apr_array_header_t *chunks = apr_array_make(r->pool, targets->nelts + 3, sizeof(chunk_t));
    for (i = 0; i < targets->nelts; i++) {
	const char *target = APR_ARRAY_IDX(targets, i, char*);
	rc = dbats_glob_keyname_start(reqstate->dh, NULL, &reqstate->dki, target);
	if (rc != 0) {
	    log_rerror(APLOG_NOTICE, reqstate,
		"mod_dbats: dbats_glob_keyname_start: %s", db_strerror(rc));
	    reqstate->dbats_status = rc;
	    reqstate->dki = NULL;
	    return;
	}
	while ((rc = dbats_glob_keyname_next(reqstate->dki, &keyid, key)) == 0) {
	    chunk_t *chunk = (chunk_t*)apr_array_push(chunks);
	    chunk->keyid = keyid;
	    chunk->key = apr_pstrdup(r->pool, key);
	    chunk->isset = apr_palloc(r->pool, nsamples * sizeof(uint8_t));
	    chunk->data = apr_palloc(r->pool, nsamples * sizeof(dbats_value));
	    log_rerror(APLOG_DEBUG, reqstate, "mod_dbats: (%d/%d key #%u %s)",
		chunks->nelts, chunks->nalloc, chunk->keyid, chunk->key);
	}
	// DB_PAGE_NOTFOUND isn't possible according to docs, but it happens,
	// and _seems_ to be the same as DB_NOTFOUND, so we treat it as such.
	if (rc == DB_PAGE_NOTFOUND) {
	    log_rerror(APLOG_NOTICE, reqstate,
		"mod_dbats: warning: dbats_glob_keyname_next: %s", db_strerror(rc));
	} else if (rc != DB_NOTFOUND) {
	    log_rerror(APLOG_NOTICE, reqstate,
		"mod_dbats: dbats_glob_keyname_next: %s", db_strerror(rc));
	    reqstate->dbats_status = rc;
	    return;
	}
	dbats_glob_keyname_end(reqstate->dki);
	reqstate->dki = NULL;
	if (reqstate->dbats_status != 0) return;
    }

    // get data
    uint32_t t = from;
    for (i = 0; i < nsamples; i++) {
	rc = dbats_select_snap(reqstate->dh, &reqstate->snap, t, 0);
	if (rc != 0) {
	    reqstate->dbats_status = rc;
	    return;
	}
	int c;
	for (c = 0; c < chunks->nelts; c++) {
	    chunk_t *chunk = &APR_ARRAY_IDX(chunks, c, chunk_t);
	    const dbats_value *v;
	    log_rerror(APLOG_DEBUG, reqstate, "mod_dbats: dbats_get key #%u %s",
		chunk->keyid, chunk->key);
	    rc = dbats_get(reqstate->snap, chunk->keyid, &v, bid);
	    if (rc == DB_NOTFOUND) {
		chunk->isset[i] = 0;
	    } else if (rc == 0) {
		chunk->isset[i] = 1;
		chunk->data[i] = v[0];
	    } else {
		log_rerror(APLOG_NOTICE, reqstate, "mod_dbats: dbats_get: %s",
		    db_strerror(rc));
		dbats_abort_snap(reqstate->snap);
		reqstate->snap = NULL;
		reqstate->dbats_status = rc;
		return;
	    }
	}
	t += bundle->period;
	dbats_commit_snap(reqstate->snap);
	reqstate->snap = NULL;
    }

    // print data
    if (format && strcmp(format, "html") == 0) {
	ap_set_content_type(r, "text/html");
	ap_rprintf(r, "<!DOCTYPE html>\n");
	ap_rprintf(r, "<html>\n");
	ap_rprintf(r, "<head>\n");
	ap_rprintf(r, "<title>DBATS data query</title>\n");
	ap_rprintf(r, "</head>\n");
	ap_rprintf(r, "<body>\n");
	ap_rprintf(r, "<h1>DBATS data query</h1>\n");
	int c;
	t = from;
	ap_rprintf(r, "<table>\n<tr><th></th>\n");
	for (i = 0; i < nsamples; i++) {
	    ap_rprintf(r, "<th>%u</th>\n", t);
	    t += bundle->period;
	}
	ap_rprintf(r, "</tr>\n");
	for (c = 0; c < chunks->nelts; c++) {
	    chunk_t *chunk = &APR_ARRAY_IDX(chunks, c, chunk_t);
	    ap_rprintf(r, "<tr><th>%s</th>\n", ap_escape_html(r->pool, chunk->key));
	    for (i = 0; i < nsamples; i++) {
		if (chunk->isset[i]) {
		    if (bundle->func == DBATS_AGG_AVG)
			ap_rprintf(r, "<td>%.16g</td>\n", chunk->data[i].d);
		    else
			ap_rprintf(r, "<td>%" PRIu64 "</td>\n", chunk->data[i].u64);
		}
	    }
	    ap_rprintf(r, "</tr>\n");
	}
	ap_rprintf(r, "</table>\n");
	ap_rprintf(r, "</body>\n");
	ap_rprintf(r, "</html>\n");

    } else if (format && strcmp(format, "json-internal") == 0) {
	ap_set_content_type(r, "application/json");
	int n = 0;
	ap_rprintf(r, "[\n");
	int c;
        uint32_t p_from = (no_pad) ? from : q_from;
        uint32_t p_until = (no_pad) ? until : q_until;
        uint32_t native_period;
        if (bid == 0) {
            native_period = bundle->period;
        } else {
            const dbats_bundle_info *native_bundle = dbats_get_bundle_info(reqstate->dh, 0);
            native_period = native_bundle->period;
        }
	for (c = 0; c < chunks->nelts; c++) {
	    chunk_t *chunk = &APR_ARRAY_IDX(chunks, c, chunk_t);
	    ap_rprintf(r, "%s{\"name\": \"%s\",\n  ",
		(n>0 ? ",\n" : ""), ap_escape_quotes(r->pool, chunk->key));
	    ap_rprintf(r, "\"start\": %u, \"step\": %u, \"nativeStep\": %u, \"end\": %u,\n  \"values\": [",
                p_from, bundle->period, native_period, p_until + bundle->period);
            for (i = 0, t = p_from; t <= p_until; t += bundle->period) {
                if (t > p_from) ap_rprintf(r, ", ");

                if (t >= from && t <= until) { // there could be a value
                    if (!chunk->isset[i]) {
                        ap_rprintf(r, "null");
                    } else if (bundle->func == DBATS_AGG_AVG) {
                        // handle buggy apache printf
                        // given 0.3 it prints .3 which breaks JSON parsers
                        // (similarly for -0.3 -> -.3)
                        char buf[64];
                        snprintf(buf, 64, "%.16g", chunk->data[i].d);
                        ap_rprintf(r, "%s", buf);
                    } else {
                        ap_rprintf(r, "%" PRIu64, chunk->data[i].u64);
                    }
                    i++;
                } else { // this is padding
                    ap_rprintf(r, "null");
                }
            }
	    ap_rprintf(r, "]}");
	    n++;
	}
	ap_rprintf(r, "\n]\n");

    } else { // default to json
	ap_set_content_type(r, "application/json");
	int n = 0;
	ap_rprintf(r, "[\n");
	int c;
	for (c = 0; c < chunks->nelts; c++) {
	    chunk_t *chunk = &APR_ARRAY_IDX(chunks, c, chunk_t);
	    ap_rprintf(r, "%s{\"target\": \"%s\", \"datapoints\": [",
		n>0 ? ",\n" : "", ap_escape_quotes(r->pool, chunk->key));
	    for (i = 0, t = from; i < nsamples; i++, t += bundle->period) {
		char valbuf[64];
		if (!chunk->isset[i]) {
		    sprintf(valbuf, "null");
		} else if (bundle->func == DBATS_AGG_AVG) {
		    sprintf(valbuf, "%.16g", chunk->data[i].d);
		} else {
		    sprintf(valbuf, "%" PRIu64, chunk->data[i].u64);
		}
		ap_rprintf(r, "%s[%s, %u]", (i==0 ? "" : ", "), valbuf, t);
	    }
	    ap_rprintf(r, "]}");
	    n++;
	}
	ap_rprintf(r, "\n]\n");
    }
}

// If {s} ends with {w}, returns a pointer to the copy of {w} within {s}.
static const char *ends_with(const char *s, const char *w)
{
    size_t sl = strlen(s);
    size_t wl = strlen(w);
    if (wl > sl) return 0;
    if (strcmp(s + sl - wl, w) == 0)
	return s + sl - wl;
    else
	return NULL;
}

static int req_handler(request_rec *r)
{
    const char *uri_tail = NULL;
    void (*func)(request_rec*, mod_dbats_reqstate*) = NULL;
    const char *dbats_path;

    if (!r->handler || strcmp(r->handler, "dbats-handler") != 0)
	return DECLINED;

    apr_threadkey_private_set(r, tlskey_req);

    // create request state
    mod_dbats_reqstate *reqstate = apr_pcalloc(r->pool, sizeof(*reqstate));
    reqstate->r = r;
    reqstate->http_status = OK;
    reqstate->dbats_status = 0;
    apr_thread_mutex_lock(procstate.reqcount_mutex);
    reqstate->id = ++procstate.reqcount;
    apr_thread_mutex_unlock(procstate.reqcount_mutex);
    ap_set_module_config(r->request_config, &dbats_module, reqstate);

    log_rerror(APLOG_INFO, reqstate, "mod_dbats: req_handler on %s:%u", r->server->server_hostname, r->server->port);
    log_rerror(APLOG_INFO, reqstate, "the_request: %s", r->the_request);
    log_rerror(APLOG_DEBUG, reqstate, "uri: %s", r->uri);
    log_rerror(APLOG_DEBUG, reqstate, "filename: %s", r->filename);
    log_rerror(APLOG_DEBUG, reqstate, "canonical_filename: %s", r->canonical_filename);
    log_rerror(APLOG_DEBUG, reqstate, "path_info: %s", r->path_info);
    log_rerror(APLOG_DEBUG, reqstate, "args: %s", r->args);

    if ((uri_tail = ends_with(r->uri, "/metrics/find/"))) {
	func = metrics_find;
    } else if ((uri_tail = ends_with(r->uri, "/metrics/index.json"))) {
	func = metrics_index;
    } else if ((uri_tail = ends_with(r->uri, "/render/"))) {
	func = render;
    } else {
	reqstate->http_status = HTTP_NOT_FOUND;
	goto done;
    }

    // get config
    mod_dbats_dir_config *cfg = (mod_dbats_dir_config*)ap_get_module_config(r->per_dir_config, &dbats_module);
    log_rerror(APLOG_DEBUG, reqstate, "dir_cfg: path=%s", cfg->path);
    if (cfg->path) {
	dbats_path = cfg->path;
    } else {
	// Generate default dbats_path by removing uri_tail from r->filename +
	// r->path_info.  (This allows a <Directory> or <DirectoryMatch>
	// without a dbatsPath directive to refer to an actual DBATS directory
	// under the document root.)  (It would be better to get the name of
	// the matching directory from apache, but AFAICT apache doesn't
	// provide it.)
	char *tmp = apr_psprintf(r->pool, "%s%s", r->filename, r->path_info);
	tmp[strlen(tmp) - strlen(uri_tail)] = '\0'; // chop off tail
	dbats_path = tmp;
	log_rerror(APLOG_DEBUG, reqstate, "default path=%s", dbats_path);
    }

    // parse query parameters
    args_to_table(r, &reqstate->queryparams);

    // We should have a log level per virtual server, but does anybody care?
    int level = r->server->log.level;
    dbats_log_level =
	level > APLOG_DEBUG ? DBATS_LOG_FINEST :
	level >= APLOG_DEBUG ? DBATS_LOG_FINEST :
	level >= APLOG_INFO ? DBATS_LOG_CONFIG :
	level >= APLOG_NOTICE ? DBATS_LOG_INFO :
	level >= APLOG_WARNING ? DBATS_LOG_WARN :
	level >= APLOG_ERR ? DBATS_LOG_ERR : 0;

    // get dbats_handler for this {process, dbats_path}
    apr_thread_mutex_lock(procstate.dht_mutex);
    reqstate->dh = (dbats_handler*)apr_hash_get(procstate.dht, dbats_path, APR_HASH_KEY_STRING);
    if (reqstate->dh) {
	apr_thread_mutex_unlock(procstate.dht_mutex);
    } else {
	reopen:
	log_rerror(APLOG_INFO, reqstate, "mod_dbats: dbats_open %s", dbats_path);
	int rc = dbats_open(&reqstate->dh, dbats_path, 1, 60, DBATS_READONLY, 0);
	if (rc != 0) {
	    log_rerror(APLOG_NOTICE, reqstate, "mod_dbats: dbats_open: %s", db_strerror(rc));
	    apr_thread_mutex_unlock(procstate.dht_mutex);
	    reqstate->http_status = (rc == ENOENT) ? HTTP_NOT_FOUND :
		HTTP_INTERNAL_SERVER_ERROR;
	    goto done;
	}
	log_rerror(APLOG_DEBUG, reqstate, "mod_dbats: dbats_open: %s", "ok");
	apr_hash_set(procstate.dht, apr_pstrdup(procstate.pool, dbats_path),
	    APR_HASH_KEY_STRING, reqstate->dh);
	apr_pool_cleanup_register(procstate.pool, reqstate->dh, mod_dbats_close, NULL);
	apr_thread_mutex_unlock(procstate.dht_mutex);
	dbats_commit_open(reqstate->dh);
	log_rerror(APLOG_DEBUG, reqstate, "mod_dbats: dbats_commit_open: %s", "ok");
    }

    // dispatch request
    func(r, reqstate);

    // clean up
    if (reqstate->dki) {
	dbats_glob_keyname_end(reqstate->dki);
	reqstate->dki = NULL;
    }
    if (reqstate->snap) {
	dbats_abort_snap(reqstate->snap);
	reqstate->snap = NULL;
    }

    // handle result
    if (reqstate->dbats_status == 0) {
	goto done;

    } else if (reqstate->dbats_status == DB_NOTFOUND) {
	reqstate->http_status = HTTP_NOT_FOUND;
	goto done;

    } else if (reqstate->dbats_status == DB_RUNRECOVERY && reqstate->retries < 1) {
	// Since we're readonly, we can't do the recovery, but if database was
	// recovered externally, we have to reopen it to use it.
	log_rerror(APLOG_NOTICE, reqstate, "mod_dbats attempting to reopen %s", dbats_path);
	if (reqstate->dh)
	    apr_pool_cleanup_run(procstate.pool, reqstate->dh, mod_dbats_close);
	apr_thread_mutex_lock(procstate.dht_mutex);
	apr_hash_set(procstate.dht, dbats_path, APR_HASH_KEY_STRING, NULL);
	reqstate->http_status = OK;
	reqstate->dbats_status = 0;
	reqstate->retries++;
	goto reopen;

    } else {
	log_rerror(APLOG_WARNING, reqstate,
	    "mod_dbats aborting request due to internal error (%d) %s",
	    reqstate->dbats_status, db_strerror(reqstate->dbats_status));
	reqstate->http_status = HTTP_INTERNAL_SERVER_ERROR;
	goto done;
    }

done:
    apr_threadkey_private_set(NULL, tlskey_req);
    log_rerror(APLOG_INFO, reqstate, "request result: %d", reqstate->http_status);
    return reqstate->http_status;
}

static void *create_dir_conf(apr_pool_t *pool, char *context)
{
    mod_dbats_dir_config *cfg = apr_pcalloc(pool, sizeof(mod_dbats_dir_config));
    return cfg;
}

static void *merge_dir_conf(apr_pool_t *pool, void *vbase, void *vadd)
{
    mod_dbats_dir_config *base = (mod_dbats_dir_config *)vbase;
    mod_dbats_dir_config *add = (mod_dbats_dir_config *)vadd;

    mod_dbats_dir_config *merged = (mod_dbats_dir_config *)create_dir_conf(pool, NULL);
    merged->path = add->path ? add->path : base->path;

    return merged;
}

static void register_hooks(apr_pool_t *pool)
{
    int major, minor, patch;
    db_version(&major, &minor, &patch);
    if (major != DB_VERSION_MAJOR || minor != DB_VERSION_MINOR || patch != DB_VERSION_PATCH) {
	ap_log_perror(APLOG_MARK, APLOG_WARNING, 0, pool,
	    "mod_dbats: libdb %d.%d.%d != db.h %d.%d.%d",
	    major, minor, patch, DB_VERSION_MAJOR, DB_VERSION_MINOR, DB_VERSION_PATCH);
	return;
    }

    ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, pool, "mod_dbats initializing");
    ap_hook_handler(req_handler, NULL, NULL, APR_HOOK_MIDDLE);
    ap_hook_child_init(child_init_handler, NULL, NULL, APR_HOOK_MIDDLE);
}

static const command_rec directives[] =
{
    AP_INIT_TAKE1("dbatsPath", set_path, NULL, ACCESS_CONF, "Path to DBATS database"),
    { NULL }
};

module AP_MODULE_DECLARE_DATA dbats_module =
{
    STANDARD20_MODULE_STUFF,
    create_dir_conf,      /* Per-directory configuration handler */
    merge_dir_conf,       /* Merge handler for per-directory configurations */
    NULL,                 /* Per-server configuration handler */
    NULL,                 /* Merge handler for per-server configurations */
    directives,           /* Any directives we may have for httpd */
    register_hooks        /* Our hook registering function */
};
