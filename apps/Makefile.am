## Process this file with automake to produce Makefile.in

DBATS_CORRUPT  = @DBATS_CORRUPT@
bin_PROGRAMS   = dbats_init report-in report-out dbats_series_limit dbats_info dbats_best_bundle dbats_recover dbats_delete_key $(DBATS_CORRUPT) dbats_dump
EXTRA_PROGRAMS = dbats_corrupt
AM_CPPFLAGS    = -I$(srcdir)
LDADD          = $(top_builddir)/lib/libdbats.la $(LIBDB)
AM_LIBTOOLFLAGS= --silent

