.PHONY: FORCE

all: lib html/index.html apps 

lib: FORCE
	cd lib; $(MAKE)

apps: FORCE
	cd apps; $(MAKE)

html/index.html: include/dbats.h
	rm -r html
	doxygen

