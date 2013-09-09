#CC           = gcc -g -std=gnu99
CC           = gcc-anal -g -std=gnu99
#WARNS        =  -Wall -Wshadow -Wpointer-arith -Wmissing-prototypes -Wmissing-declarations -Wnested-externs
INCLUDE      = -I../include -I/opt/local/include
#OPTIMIZE     = -O
OPTIMIZE     = -O -DNDEBUG
#PROF         = --coverage
#PROF         = -pg
CFLAGS       = $(OPTIMIZE) $(INCLUDE) $(PROF) $(WARNS) # -fPIC
LDFLAGS      = -L . $(PROF)
LDLIBS      = -ldbats -ldb

DBATS_LIB   = libdbats.a

TARGETS     = $(DBATS_LIB)

all: $(TARGETS)

dbats.o: dbats.c ../include/dbats.h ../include/dbats_log.h Makefile

dbats_log.o: dbats_log.c ../include/dbats_log.h Makefile

$(DBATS_LIB): dbats.o dbats_log.o quicklz.o
	ar rs $@ $?
	ranlib $@

clean:
	/bin/rm -f $(TARGETS) *.o *~
