## Process this file with automake to produce Makefile.in

lib_LTLIBRARIES     = libdbats.la
libdbats_la_SOURCES = dbats.c dbats_log.c quicklz.c ../include/uint.h
noinst_HEADERS      = quicklz.h
EXTRA_DIST          = mod_dbats.c
EXTRA_DIST         += $(top_srcdir)/./config.rpath
CLEANFILES          = mod_dbats.la mod_dbats.slo
libdbats_la_LIBADD  = $(LTLIBDB)
libdbats_la_LDFLAGS = -version-info 0:0:0
AM_LIBTOOLFLAGS     = --silent

APXS                = @APXS@
APACHECTL           = @APACHECTL@
A2DISMOD            = @A2DISMOD@

DBATS_V_APXS        = $(dbats__v_APXS_@AM_V@)
dbats__v_APXS_      = $(dbats__v_APXS_@AM_DEFAULT_V@)
dbats__v_APXS_0     = @echo "  APXS  " $@;


all-local: $(MOD_DBATS)

# in case of a VPATH build
mod_dbats.c: Makefile
	test -r mod_dbats.c || $(LN_S) $(srcdir)/mod_dbats.c mod_dbats.c

mod_dbats.la: mod_dbats.c libdbats.la
	$(DBATS_V_APXS)$(APXS) -c $(APXS_DEBUG) $(AM_CPPFLAGS) $(CPPFLAGS) -L. $(AM_LDFLAGS) $(LDFLAGS) -ldbats mod_dbats.c $(LTLIBDB)

# depends on "install" because mod_dbats will need to load INSTALLED libdbats.so
install-apache: $(MOD_DBATS) install
	@runtimedir="$$($(APXS) -q runtimedir)"; \
	progname="$$($(APXS) -q progname)"; \
	for file in "$$runtimedir/$$progname.pid" "/var/run/$$progname.pid"; do \
	    if test -f "$$file"; then pidfile="$$file"; break; fi; \
	done; \
	running=''; \
	if kill -0 $$(cat "$$pidfile") 2>/dev/null; then \
	    running=1; \
	    if $(APACHECTL) -M 2>/dev/null | egrep dbats_module >/dev/null; then \
		echo "# Attempting to disable existing dbats_module..."; \
		if test -n "$(A2DISMOD)"; then \
		    cmd="$(A2DISMOD) dbats"; \
		else \
		    cmd="$(APXS) -e -A mod_dbats.la"; \
		fi; \
		echo "$$cmd"; \
		$$cmd || exit $$?; \
		if $(APACHECTL) -M 2>/dev/null | egrep dbats_module >/dev/null; then \
		    echo "#############################################################################"; \
		    echo "### Another dbats module appears to be loaded in the Apache server process."; \
		    echo "### You should either stop the server with \"$(APACHECTL) stop\","; \
		    echo "### or disable the module (by commenting out the LoadModule line) and restart"; \
		    echo "### the server with \"$(APACHECTL) graceful\"."; \
		    echo "#############################################################################"; \
		    exit 1; \
		fi; \
		echo "$(APACHECTL) graceful"; \
		$(APACHECTL) graceful || exit $$?; \
		sleep 5; \
	    fi; \
	fi; \
	echo "$(APXS) -a -i mod_dbats.la"; \
	$(APXS) -a -i mod_dbats.la || exit $$?; \
	if test -n "$$running"; then \
	    echo "# Restarting Apache server with new dbats_module..."; \
	    echo "$(APACHECTL) graceful"; \
	    $(APACHECTL) graceful || exit $$?; \
	fi

#	$(DBATS_V_APXS)$(APXS) -a -i mod_dbats.la
