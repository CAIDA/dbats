/*
 * DBATS module for Apache 2.2 web server
 */

#include "httpd.h"
#include "http_config.h"
#include "http_protocol.h"
#include "http_log.h"
#include "ap_config.h"
#include "apr_strings.h"
#include "apr_hash.h"
#include "dbats.h"

typedef struct {
    const char *context;
    const char *path;
    int n;
} mod_dbats_dir_config;

struct {
    apr_pool_t *pool;
    apr_thread_mutex_t *mutex;
    apr_hash_t *dht; // hash table of dbats_path -> dbats_handler
} procstate;

module AP_MODULE_DECLARE_DATA dbats_module;

static const char *set_path(cmd_parms *cmd, void *vcfg, const char *arg)
{
    mod_dbats_dir_config *cfg = (mod_dbats_dir_config*)vcfg;
    cfg->path = arg;
    return NULL;
}

static apr_status_t mod_dbats_close(void *data)
{
    dbats_handler *dh = (dbats_handler*)data;
    ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, procstate.pool, "mod_dbats: mod_dbats_close pid=%u:%lu", getpid(), pthread_self());
    int rc = dbats_close(dh);
    ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, procstate.pool, "mod_dbats: dbats_close: %d", rc);
    return APR_SUCCESS;
}

static void child_init_handler(apr_pool_t *pchild, server_rec *s)
{
    ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, pchild, "############ mod_dbats: child_init_handler pchild=%08x pid=%u:%lu", (unsigned)pchild, getpid(), pthread_self());
    dbats_log_level = DBATS_LOG_FINE;
    procstate.dht = apr_hash_make(pchild);
    procstate.pool = pchild;
    apr_thread_mutex_create(&procstate.mutex, APR_THREAD_MUTEX_DEFAULT, pchild);
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
	ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, r->pool, "query param: %s[%d]=%s",
	    name, plist->nelts, value);
	APR_ARRAY_PUSH(plist, char*) = value;
    }
}

static inline char *get_first_param(apr_table_t *queryparams, const char *key)
{
    apr_array_header_t *plist = (apr_array_header_t*)apr_table_get(queryparams, key);
    return plist ? APR_ARRAY_IDX(plist, 0, char*) : NULL;
}

typedef struct {
    apr_table_t *queryparams;
    dbats_handler *dh;
} mod_dbats_reqstate;

static int metrics_find(request_rec *r)
{
    mod_dbats_reqstate *reqstate = (mod_dbats_reqstate*)
	ap_get_module_config(r->request_config, &dbats_module);

    int result = OK;
    int rc;

    const char *query = get_first_param(reqstate->queryparams, "query");
    const char *format = get_first_param(reqstate->queryparams, "format");
    //const char *logLevel = get_first_param(reqstate->queryparams, "logLevel");

    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# query: %s", query);

    dbats_keytree_iterator *dki;
    uint32_t keyid;
    char name[DBATS_KEYLEN];

    rc = dbats_glob_keyname_start(reqstate->dh, NULL, &dki, query);
    if (rc != 0) {
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_glob_keyname_start: %s", db_strerror(rc));
	return HTTP_INTERNAL_SERVER_ERROR;
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
	while ((rc = dbats_glob_keyname_next(dki, &keyid, name)) == 0) {
	    ap_rprintf(r, "<li>%s\n", ap_escape_html(r->pool, name));
	}
	ap_rprintf(r, "</ul>\n");
	ap_rprintf(r, "</body>\n");
	ap_rprintf(r, "</html>\n");

    } else { // default to json
	ap_set_content_type(r, "application/json");
	int n = 0;
	ap_rprintf(r, "[");
	while ((rc = dbats_glob_keyname_next(dki, &keyid, name)) == 0) {
	    const char *ename = ap_escape_quotes(r->pool, name);
	    const char *lastpart = strrchr(ename, '.');
	    lastpart = lastpart ? lastpart + 1 : ename;
	    int isleaf = !(keyid & DBATS_KEY_IS_PREFIX);
	    ap_rprintf(r, "%s\n    {", n ? "," : "");
	    ap_rprintf(r, "\"text\": \"%s\", ", lastpart);
	    ap_rprintf(r, "\"expandable\": %d, ", !isleaf);
	    ap_rprintf(r, "\"leaf\": %d, ", isleaf);
	    ap_rprintf(r, "\"id\": \"%s\", ", ename);
	    ap_rprintf(r, "\"allowChildren\": %d}", !isleaf);
	    n++;
	}
	ap_rprintf(r, "\n]\n");
    }

    if (rc != DB_NOTFOUND) {
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r,
	    "mod_dbats: dbats_glob_keyname_next: %s", db_strerror(rc));
	result = HTTP_INTERNAL_SERVER_ERROR;
    }

    dbats_glob_keyname_end(dki);

    return result;
}

static int metrics_index(request_rec *r)
{
    mod_dbats_reqstate *reqstate = (mod_dbats_reqstate*)
	ap_get_module_config(r->request_config, &dbats_module);

    int result = OK;
    int rc;

    dbats_keytree_iterator *dki;
    uint32_t keyid;
    char name[DBATS_KEYLEN];

    rc = dbats_glob_keyname_start(reqstate->dh, NULL, &dki, NULL);
    if (rc != 0) {
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_glob_keyname_start: %s", db_strerror(rc));
	return HTTP_INTERNAL_SERVER_ERROR;
    }

    ap_set_content_type(r, "application/json");
    int n = 0;
    ap_rprintf(r, "[");
    while ((rc = dbats_glob_keyname_next(dki, &keyid, name)) == 0) {
	ap_rprintf(r, "%s\n    \"%s\"",
	    n ? "," : "", ap_escape_quotes(r->pool, name));
	n++;
    }
    ap_rprintf(r, "\n]\n");

    if (rc != DB_NOTFOUND) {
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r,
	    "mod_dbats: dbats_glob_keyname_next: %s", db_strerror(rc));
	result = HTTP_INTERNAL_SERVER_ERROR;
    }

    dbats_glob_keyname_end(dki);

    return result;
}

static int render(request_rec *r)
{
    mod_dbats_reqstate *reqstate = (mod_dbats_reqstate*)
	ap_get_module_config(r->request_config, &dbats_module);

    int result = OK;
    int rc;
    int bid = 0;
    int i;
    uint32_t from, until, max_points = 0;
    int nsamples = 0;
    const dbats_bundle_info *bundle = NULL;

    apr_array_header_t *targets = (apr_array_header_t*)apr_table_get(reqstate->queryparams, "target");
    const char *str_from = get_first_param(reqstate->queryparams, "from");
    const char *str_until = get_first_param(reqstate->queryparams, "until");
    const char *str_max_points = get_first_param(reqstate->queryparams, "maxDataPoints");
    const char *format = get_first_param(reqstate->queryparams, "format");
    //const char *logLevel = get_first_param(reqstate->queryparams, "logLevel");

    char *p;
    from = strtol(str_from, &p, 10);
    if (!*str_from || *p) return HTTP_BAD_REQUEST;
    until = strtol(str_until, &p, 10);
    if (!*str_until || *p) return HTTP_BAD_REQUEST;
    if (str_max_points) {
	max_points = strtol(str_max_points, &p, 10);
	if (!*str_max_points || *p) return HTTP_BAD_REQUEST;
    }

    for (i = 0; i < targets->nelts; i++)
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# target[%d]: %s", i, APR_ARRAY_IDX(targets, i, char*));

    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# from:   %" PRIu32, from);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# until:  %" PRIu32, until);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# max_points: %" PRIu32, max_points);

    if (max_points > 0) {
	bid = dbats_best_bundle(reqstate->dh, DBATS_AGG_SUM, from, until, max_points);
	if (bid < 0)
	    return HTTP_NOT_FOUND; // XXX ?
    }

    dbats_normalize_time(reqstate->dh, bid, &from);
    dbats_normalize_time(reqstate->dh, bid, &until);

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
	return HTTP_INTERNAL_SERVER_ERROR;
    }

    bundle = dbats_get_bundle_info(reqstate->dh, bid);
    nsamples = (until < from) ? 0 : (until - from) / bundle->period + 1;

    typedef struct {
	const char *key;
	uint32_t keyid;
	dbats_value *data;
	uint8_t *isset;
    } chunk_t;

    // get keys
    dbats_keytree_iterator *dki;
    char key[DBATS_KEYLEN];
    uint32_t keyid;

    apr_array_header_t *chunks = apr_array_make(r->pool, targets->nelts + 3, sizeof(chunk_t));
    for (i = 0; i < targets->nelts; i++) {
	const char *target = APR_ARRAY_IDX(targets, i, char*);
	rc = dbats_glob_keyname_start(reqstate->dh, NULL, &dki, target);
	if (rc != 0) {
	    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r,
		"mod_dbats: dbats_glob_keyname_start: %s", db_strerror(rc));
	    return HTTP_INTERNAL_SERVER_ERROR;
	}
	while ((rc = dbats_glob_keyname_next(dki, &keyid, key)) == 0) {
	    chunk_t *chunk = (chunk_t*)apr_array_push(chunks);
	    chunk->keyid = keyid;
	    chunk->key = apr_pstrdup(r->pool, key);
	    chunk->isset = apr_palloc(r->pool, nsamples * sizeof(uint8_t));
	    chunk->data = apr_palloc(r->pool, nsamples * sizeof(dbats_value));
	    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: (%d/%d) key #%u %s",
		chunks->nelts, chunks->nalloc, chunk->keyid, chunk->key);
	}
	if (rc != DB_NOTFOUND) {
	    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r,
		"mod_dbats: dbats_glob_keyname_next: %s", db_strerror(rc));
	    result = HTTP_INTERNAL_SERVER_ERROR;
	}
	dbats_glob_keyname_end(dki);
	if (result != OK) return result;
    }

    // get data
    uint32_t t = from;
    dbats_snapshot *snap;
    for (i = 0; i < nsamples; i++) {
	rc = dbats_select_snap(reqstate->dh, &snap, t, 0);
	if (rc != 0)
	    return HTTP_INTERNAL_SERVER_ERROR;
	int c;
	for (c = 0; c < chunks->nelts; c++) {
	    chunk_t *chunk = &APR_ARRAY_IDX(chunks, c, chunk_t);
	    const dbats_value *p;
	    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_get key #%u %s",
		chunk->keyid, chunk->key);
	    rc = dbats_get(snap, chunk->keyid, &p, bid);
	    if (rc == DB_NOTFOUND) {
		chunk->isset[i] = 0;
	    } else if (rc == 0) {
		chunk->isset[i] = 1;
		chunk->data[i] = p[0];
	    } else {
		dbats_abort_snap(snap);
		return HTTP_INTERNAL_SERVER_ERROR;
	    }
	}
	t += bundle->period;
	dbats_commit_snap(snap);
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
		    ap_rprintf(r, "<td>%" PRIval "</td>\n", chunk->data[i]);
		}
	    }
	    ap_rprintf(r, "</tr>\n");
	}
	ap_rprintf(r, "</table>\n");
	ap_rprintf(r, "</body>\n");
	ap_rprintf(r, "</html>\n");

    } else { // default to json
	ap_set_content_type(r, "application/json");
	int n = 0;

	ap_rprintf(r, "[");
	int c;
	for (c = 0; c < chunks->nelts; c++) {
	    chunk_t *chunk = &APR_ARRAY_IDX(chunks, c, chunk_t);
	    ap_rprintf(r, "%s{\"target\": \"%s\", \"datapoints\": [",
		n>0 ? ", " : "", ap_escape_quotes(r->pool, chunk->key));
	    t = from;
	    int first = 1;
	    for (i = 0; i < nsamples; i++) {
		if (chunk->isset[i]) {
		    ap_rprintf(r, "%s[%" PRIval ", %u]", first ? "" : ", ", chunk->data[i], t);
		    first = 0;
		}
		t += bundle->period;
	    }
	    ap_rprintf(r, "]}");
	    n++;
	}
	ap_rprintf(r, "]\n");
    }

    return result;
}

static int ends_with(const char *s, const char *w)
{
    size_t sl = strlen(s);
    size_t wl = strlen(w);
    if (wl > sl) return 0;
    return strcmp(s + sl - wl, w) == 0;
}

static int req_handler(request_rec *r)
{
    if (!r->handler || strcmp(r->handler, "dbats-handler") != 0)
	return DECLINED;

    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "######## mod_dbats: req_handler pid=%u:%lu", getpid(), pthread_self());
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# the_request: %s", r->the_request);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# uri: %s", r->uri);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# filename: %s", r->filename);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# canonical_filename: %s", r->canonical_filename);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# path_info: %s", r->path_info);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# args: %s", r->args);

    // get config
    mod_dbats_dir_config *cfg = (mod_dbats_dir_config*)ap_get_module_config(r->per_dir_config, &dbats_module);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "dir_cfg: %08x, n=%d, context=%s, path=%s", (unsigned)cfg, ++cfg->n, cfg->context, cfg->path);

    // create request state
    mod_dbats_reqstate *reqstate = apr_pcalloc(r->pool, sizeof(*reqstate));
    ap_set_module_config(r->request_config, &dbats_module, reqstate);

    // parse query parameters
    args_to_table(r, &reqstate->queryparams);

    // get dbats_handler for cfg->path
    apr_thread_mutex_lock(procstate.mutex);
    reqstate->dh = (dbats_handler*)apr_hash_get(procstate.dht, cfg->path, APR_HASH_KEY_STRING);
    if (reqstate->dh) {
	apr_thread_mutex_unlock(procstate.mutex);
    } else {
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "######## mod_dbats: dbats_open %s", cfg->path);
	int rc = dbats_open(&reqstate->dh, cfg->path, 1, 60, DBATS_READONLY, 0);
	if (rc != 0) {
	    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_open: %s", db_strerror(rc));
	    apr_thread_mutex_unlock(procstate.mutex);
	    return HTTP_INTERNAL_SERVER_ERROR;
	}
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_open: ok");
	apr_hash_set(procstate.dht, cfg->path, APR_HASH_KEY_STRING, reqstate->dh);
	apr_pool_cleanup_register(procstate.pool, reqstate->dh, mod_dbats_close, NULL);
	apr_thread_mutex_unlock(procstate.mutex);
	dbats_commit_open(reqstate->dh);
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_commit_open: ok");
    }

    // dispatch request
    if (ends_with(r->uri, "/metrics/find/")) {
	return metrics_find(r);
    } else if (ends_with(r->uri, "/metrics/index.json")) {
	return metrics_index(r);
    } else if (ends_with(r->uri, "/render/")) {
	return render(r);
    } else {
	return HTTP_NOT_FOUND;
    }
}

static void *create_dir_conf(apr_pool_t *pool, char *context)
{
    ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, pool, "create_dir_conf: pid=%u:%lu, context=%s", getpid(), pthread_self(), context);
    mod_dbats_dir_config *cfg = apr_pcalloc(pool, sizeof(mod_dbats_dir_config));
    if (cfg) {
	cfg->path = NULL;
	cfg->context = apr_pstrdup(pool, context);
	cfg->n = 0;
    }
    return cfg;
}

static void *merge_dir_conf(apr_pool_t *pool, void *vbase, void *vadd)
{
    mod_dbats_dir_config *base = (mod_dbats_dir_config *)vbase;
    mod_dbats_dir_config *add = (mod_dbats_dir_config *)vadd;

    char *context = apr_psprintf(pool, "(%s) + (%s)", base->context, add->context);
    mod_dbats_dir_config *merged = (mod_dbats_dir_config *)create_dir_conf(pool, context);
    merged->path = add->path ? add->path : base->path;

    return merged;
}

static void register_hooks(apr_pool_t *pool)
{
    int major, minor;
    db_version(&major, &minor, NULL);
    if (major != DB_VERSION_MAJOR || minor != DB_VERSION_MINOR) {
	ap_log_perror(APLOG_MARK, APLOG_WARNING, 0, pool,
	    "mod_dbats: libdb %d.%d != db.h %d.%d",
	    major, minor, DB_VERSION_MAJOR, DB_VERSION_MINOR);
	return;
    }

    ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, pool, "mod_dbats: register_hooks");
    ap_hook_handler(req_handler, NULL, NULL, APR_HOOK_MIDDLE);
    ap_hook_child_init(child_init_handler, NULL, NULL, APR_HOOK_MIDDLE);
}

static const command_rec directives[] =
{
    AP_INIT_TAKE1("dbatsPath", set_path, NULL, OR_ALL, "Path to DBATS database"),
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
