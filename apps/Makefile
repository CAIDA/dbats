#CC           = gcc -g -std=gnu99
CC           = gcc-anal -g -std=gnu99
#WARNS        =  -Wall -Wshadow -Wpointer-arith -Wmissing-prototypes -Wmissing-declarations -Wnested-externs
INCLUDE      = -I../include -I /opt/local/include
#OPTIMIZE     = -O
OPTIMIZE     = -O -DNDEBUG
#PROF         = --coverage
#PROF         = -pg
CFLAGS       = $(OPTIMIZE) $(INCLUDE) $(PROF) $(WARNS) # -fPIC
LDFLAGS      = -L../lib $(PROF)
LDLIBS      = -ldbats -ldb

TARGETS     = report-in report-out dbats_series_limit dbats_info

all: $(TARGETS)

clean:
	/bin/rm -f $(TARGETS) *.o *~

DBATS_LIB = ../lib/libdbats.a

report-in.o: report-in.c ../include/dbats.h ../include/dbats_log.h Makefile

report-out.o: report-out.c ../include/dbats.h ../include/dbats_log.h Makefile

dbats_series_limit.o: dbats_series_limit.c ../include/dbats.h ../include/dbats_log.h Makefile

dbats_info.o: dbats_info.c ../include/dbats.h ../include/dbats_log.h Makefile

report-in: report-in.o $(DBATS_LIB) Makefile
	$(CC) $(LDFLAGS) report-in.o $(LDLIBS) -o $@

report-out: report-out.o $(DBATS_LIB) Makefile
	$(CC) $(LDFLAGS) report-out.o $(LDLIBS) -o $@

dbats_series_limit: dbats_series_limit.o $(DBATS_LIB) Makefile
	$(CC) $(LDFLAGS) dbats_series_limit.o $(LDLIBS) -o $@

dbats_info: dbats_info.o $(DBATS_LIB) Makefile
	$(CC) $(LDFLAGS) dbats_info.o $(LDLIBS) -o $@
