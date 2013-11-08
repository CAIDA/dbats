## Process this file with automake to produce Makefile.in

lib_LTLIBRARIES     = libdbats.la
libdbats_la_SOURCES = dbats.c dbats_log.c quicklz.c ../include/uint.h
AM_CPPFLAGS         = -I../include

APXS                = @APXS@

all-local: mod_dbats.la

mod_dbats.la: mod_dbats.c libdbats.la
	$(APXS) -c $(APXS_DEBUG) $(AM_CPPFLAGS) $(CPPFLAGS) -L. $(AM_LDFLAGS) $(LDFLAGS) -ldbats mod_dbats.c

install-exec-local: mod_dbats.la
	$(APXS) -a -i mod_dbats.la

