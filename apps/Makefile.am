## Process this file with automake to produce Makefile.in

DBATS_CORRUPT  = @DBATS_CORRUPT@
bin_PROGRAMS   = report-in report-out dbats_series_limit dbats_info dbats_best_bundle dbats_recover $(DBATS_CORRUPT)
EXTRA_PROGRAMS = dbats_corrupt
AM_CPPFLAGS    = -I../include
LDADD          = ../lib/libdbats.a

#report_in_SOURCES:          report-in.c
#report_out_SOURCES:         report-out.c
#dbats_series_limit_SOURCES: dbats_series_limit.c
#dbats_info_SOURCES:         dbats_info.c
#dbats_best_bundle_SOURCES:  dbats_best_bundle.c
#dbats_recover_SOURCES:      dbats_recover.c

############################################

#CC           = @CC@
##PROF         = --coverage
##PROF         = -pg
#CPPFLAGS     = @CPPFLAGS@ -I../include
#CFLAGS       = @CFLAGS@ $(PROF) $(CPPFLAGS) # -fPIC
#LDFLAGS      = -L../lib @LDFLAGS@ $(PROF)
#LDLIBS       = -ldbats @LIBS@
#LD_RUN_PATH  = @LD_RUN_PATH@
#
#all: $(TARGETS)
#
#clean:
#	/bin/rm -f $(TARGETS) *.o *~
#
#DBATS_LIB = ../lib/libdbats.a
#
#.o:
#	env LD_RUN_PATH="$(LD_RUN_PATH)" $(CC) $(CFLAGS) $(LDFLAGS) $< $(LDLIBS) -o $@
#
#report-in.o: report-in.c ../include/dbats.h ../include/dbats_log.h Makefile
#report-in: report-in.o $(DBATS_LIB) Makefile
#
#report-out.o: report-out.c ../include/dbats.h ../include/dbats_log.h Makefile
#report-out: report-out.o $(DBATS_LIB) Makefile
#
#dbats_series_limit.o: dbats_series_limit.c ../include/dbats.h ../include/dbats_log.h Makefile
#dbats_series_limit: dbats_series_limit.o $(DBATS_LIB) Makefile
#
#dbats_info.o: dbats_info.c ../include/dbats.h ../include/dbats_log.h Makefile
#dbats_info: dbats_info.o $(DBATS_LIB) Makefile
#
#dbats_best_bundle.o: dbats_best_bundle.c ../include/dbats.h ../include/dbats_log.h Makefile
#dbats_best_bundle: dbats_best_bundle.o $(DBATS_LIB) Makefile
#
#dbats_recover.o: dbats_recover.c ../include/dbats.h ../include/dbats_log.h Makefile
#dbats_recover: dbats_recover.o $(DBATS_LIB) Makefile
#
