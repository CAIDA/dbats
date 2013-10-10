/*
 * DBATS module for Apache 2.2 web server
 */

#include "httpd.h"
#include "http_config.h"
#include "http_protocol.h"
#include "http_log.h"
#include "ap_config.h"
#include "apr_strings.h"
#include "dbats.h"

typedef struct {
    const char *path;
    dbats_handler *dh;
} mod_dbats_config;

module AP_MODULE_DECLARE_DATA dbats_module;

static const char *set_path(cmd_parms *cmd, void *vcfg, const char *arg)
{
    mod_dbats_config *cfg = (mod_dbats_config*)vcfg;
    cfg->path = arg;
    return NULL;
}

static void child_init_handler(apr_pool_t *pchild, server_rec *s)
{
    //mod_dbats_config *cfg = (mod_dbats_config*)ap_get_module_config(r->per_dir_config, &dbats_module);
    ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, pchild, "mod_dbats: child_init_handler pid=%u", getpid());
    //dbats_log_level = 99;
    //int rc = dbats_open(&cfg->dh, cfg->path, 1, 60, DBATS_READONLY);
    //ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_open returned %d: %s", rc, db_strerror(rc));
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
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: req_handler pid=%u", getpid());
    //dbats_log_level = 99;

    mod_dbats_config *cfg = (mod_dbats_config*)ap_get_module_config(r->per_dir_config, &dbats_module);

    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_open %s", cfg->path);
    int rc = dbats_open(&cfg->dh, cfg->path, 1, 60, DBATS_READONLY);
    if (rc != 0) {
	ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_open: %s", db_strerror(rc));
	return HTTP_INTERNAL_SERVER_ERROR;
    }
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_open: ok");
    dbats_commit_open(cfg->dh);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "mod_dbats: dbats_commit_open: ok");

    apr_table_t *GET;
    args_to_table(r, &GET);
    const char *query = apr_table_get(GET, "query");
    const char *format = apr_table_get(GET, "format");
    const char *logLevel = apr_table_get(GET, "logLevel");

#if 1
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# query: %s", query);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# format: %s", format);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# logLevel: %s", logLevel);
#endif

    dbats_keytree_iterator *dki;
    uint32_t keyid;
    char name[DBATS_KEYLEN];

    rc = dbats_glob_keyname_start(cfg->dh, NULL, &dki, query);
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
    dbats_close(cfg->dh);

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

    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# the_request: %s", r->the_request);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# hostname: %s", r->hostname);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# status_line: %s", r->status_line);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# method: %s", r->method);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# content_encoding: %s", r->content_encoding);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# unparsed_uri: %s", r->unparsed_uri);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# uri: %s", r->uri);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# filename: %s", r->filename);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# canonical_filename: %s", r->canonical_filename);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# path_info: %s", r->path_info);
    ap_log_rerror(APLOG_MARK, APLOG_NOTICE, 0, r, "# args: %s", r->args);

    if (ends_with(r->uri, "/metrics/find/"))
	return metrics_find(r);
    return HTTP_NOT_FOUND;
}

static void *create_dir_conf(apr_pool_t *pool, char *context)
{
    if (!context) context = "(undefined context)";
    mod_dbats_config *cfg = apr_pcalloc(pool, sizeof(mod_dbats_config));
    if (cfg) {
	cfg->path = NULL;
    }
    return cfg;
}

static void register_hooks(apr_pool_t *pool)
{
#if 1
    int major, minor;
    db_version(&major, &minor, NULL);
    if (major != DB_VERSION_MAJOR || minor != DB_VERSION_MINOR) {
	ap_log_perror(APLOG_MARK, APLOG_WARNING, 0, pool,
	    "mod_dbats: libdb %d.%d != db.h %d.%d",
	    major, minor, DB_VERSION_MAJOR, DB_VERSION_MINOR);
	return;
    }
#endif

    ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, pool, "mod_dbats: register_hooks");
    //dbats_log_level = 99;
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
    NULL,                 /* Merge handler for per-directory configurations */
    NULL,                 /* Per-server configuration handler */
    NULL,                 /* Merge handler for per-server configurations */
    directives,           /* Any directives we may have for httpd */
    register_hooks        /* Our hook registering function */
};
