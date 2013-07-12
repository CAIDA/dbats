#CC           = gcc -g -std=gnu99
CC           = gcc-anal -g -std=gnu99
#WARNS        =  -Wshadow -Wpointer-arith -Wmissing-prototypes -Wmissing-declarations -Wnested-externs
INCLUDE      = -I. -I /opt/local/include $(WARNS)
#OPTIMIZE     = -O
CFLAGS       = $(OPTIMIZE) -Wall $(INCLUDE) # -fPIC
LDFLAGS      = -L .
#DMALLOCLIBS  = dmalloc/malloc.o dmalloc/dmalloc.o
LDLIBS      = -ldbats $(DMALLOCLIBS) -ldb

DBATS_LIB   = libdbats.a

TARGETS     = $(DBATS_LIB) report-in report-out

all: $(TARGETS)

dbats.o: dbats.c dbats.h dbats_log.h Makefile

dbats_log.o: dbats_log.c dbats_log.h Makefile

$(DBATS_LIB): dbats.o dbats_log.o quicklz.o
	ar rs $@ $?
	ranlib $@

clean:
	/bin/rm -f $(TARGETS) *.o *~

report-in.o: report-in.c dbats.h dbats_log.h Makefile

report-out.o: report-out.c dbats.h dbats_log.h Makefile

report-in: report-in.o $(DBATS_LIB) Makefile
	$(CC) $(LDFLAGS) report-in.o $(LDLIBS) -o $@

report-out: report-out.o $(DBATS_LIB) Makefile
	$(CC) $(LDFLAGS) report-out.o $(LDLIBS) -o $@
