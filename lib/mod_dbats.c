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

// Parse URL query parameters into a table
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
	apr_table_addn(*tablep, name, value);
    }
}

static int metrics_find(request_rec *r)
{
    int result = OK;
    int rc;

    mod_dbats_dir_config *cfg = (mod_dbats_dir_config*)ap_get_module_config(r->per_dir_config, &dbats_module);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "dir_cfg: %08x, n=%d, context=%s, path=%s", (unsigned)cfg, ++cfg->n, cfg->context, cfg->path);

    apr_thread_mutex_lock(procstate.mutex);
    dbats_handler *dh = (dbats_handler*)apr_hash_get(procstate.dht, cfg->path, APR_HASH_KEY_STRING);
    if (dh) {
	apr_thread_mutex_unlock(procstate.mutex);
    } else {
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "######## mod_dbats: dbats_open %s", cfg->path);
	rc = dbats_open(&dh, cfg->path, 1, 60, DBATS_READONLY);
	if (rc != 0) {
	    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_open: %s", db_strerror(rc));
	    apr_thread_mutex_unlock(procstate.mutex);
	    return HTTP_INTERNAL_SERVER_ERROR;
	}
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_open: ok");
	apr_hash_set(procstate.dht, cfg->path, APR_HASH_KEY_STRING, dh);
	apr_pool_cleanup_register(procstate.pool, dh, mod_dbats_close, NULL);
	apr_thread_mutex_unlock(procstate.mutex);
	dbats_commit_open(dh);
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_commit_open: ok");
    }

    apr_table_t *GET;
    args_to_table(r, &GET);
    const char *query = apr_table_get(GET, "query");
    const char *format = apr_table_get(GET, "format");
    const char *logLevel = apr_table_get(GET, "logLevel");

    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# query: %s", query);

    dbats_keytree_iterator *dki;
    uint32_t keyid;
    char name[DBATS_KEYLEN];

    rc = dbats_glob_keyname_start(dh, NULL, &dki, query);
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

    if (ends_with(r->uri, "/metrics/find/"))
	return metrics_find(r);
    else
	return HTTP_NOT_FOUND;
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
