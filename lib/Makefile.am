## Process this file with automake to produce Makefile.in

lib_LTLIBRARIES     = libdbats.la
libdbats_la_SOURCES = dbats.c dbats_log.c quicklz.c ../include/uint.h
noinst_HEADERS      = quicklz.h
EXTRA_DIST          = mod_dbats.c
CLEANFILES          = mod_dbats.la mod_dbats.slo

APXS                = @APXS@

all-local: $(MOD_DBATS)

# in case of a VPATH build
mod_dbats.c: Makefile
	test -r mod_dbats.c || $(LN_S) $(srcdir)/mod_dbats.c mod_dbats.c

mod_dbats.la: mod_dbats.c libdbats.la
	$(APXS) -c $(APXS_DEBUG) $(AM_CPPFLAGS) $(CPPFLAGS) -L. $(AM_LDFLAGS) $(LDFLAGS) -ldbats mod_dbats.c

install-apache: $(MOD_DBATS)
	$(APXS) -a -i mod_dbats.la

