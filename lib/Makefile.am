## Process this file with automake to produce Makefile.in

lib_LIBRARIES      = libdbats.a
libdbats_a_SOURCES = dbats.c dbats_log.c quicklz.c
AM_CPPFLAGS        = -I../include

############################################

#CC           = @CC@
##PROF         = --coverage
##PROF         = -pg
#CPPFLAGS     = @CPPFLAGS@ -I../include
#CFLAGS       = @CFLAGS@ $(PROF) $(CPPFLAGS) # -fPIC
#LDFLAGS      = @LDFLAGS@ $(PROF)
#LDLIBS       = @LIBS@
#RANLIB       = @RANLIB@
#APXS         = @APXS@
#APXS_DEBUG   = @APXS_DEBUG@
#LIBTOOL      = @LIBTOOL@
#LD_RUN_PATH  = @LD_RUN_PATH@
#
#TARGETS     = libdbats.a @MOD_DBATS@
#
#
#all: $(TARGETS)
#
#dbats.o: dbats.c ../include/dbats.h ../include/dbats_log.h Makefile
#
#dbats.lo: dbats.c ../include/dbats.h ../include/dbats_log.h Makefile
#	$(LIBTOOL) --mode=compile $(CC) -c $(CPPFLAGS) $(CFLAGS) dbats.c
#
#dbats_log.o: dbats_log.c ../include/dbats_log.h Makefile
#
#dbats_log.lo: dbats_log.c ../include/dbats_log.h Makefile
#	$(LIBTOOL) --mode=compile $(CC) -c $(CPPFLAGS) $(CFLAGS) dbats_log.c
#
#quicklz.o: quicklz.c Makefile
#
#quicklz.lo: quicklz.c Makefile
#	$(LIBTOOL) --mode=compile $(CC) -c $(CPPFLAGS) $(CFLAGS) quicklz.c
#
#libdbats.a: dbats.o dbats_log.o quicklz.o
#	$(AR) rs $@ $?
#	$(RANLIB) $@
#
#libdbats.la: dbats.lo dbats_log.lo quicklz.lo
#	$(LIBTOOL) --mode=link $(CC) $(CFLAGS) -L. $(LDFLAGS) $(LDLIBS) -o $@ $?
#
#mod_dbats.la: mod_dbats.c libdbats.la
#	env LD_RUN_PATH="$(LD_RUN_PATH)" $(APXS) -c $(APXS_DEBUG) $(CPPFLAGS) -L. $(LDFLAGS) -ldbats $(LDLIBS) mod_dbats.c
#
#install: mod_dbats.la
#	$(APXS) -a -i mod_dbats.la
#
#clean:
#	/bin/rm -f $(TARGETS) *.o *~
#	test -n "$(LIBTOOL)" && $(LIBTOOL) --mode=clean /bin/rm -f *.lo
