all: lib html/index.html apps 

clean: FORCE
	cd lib; $(MAKE) clean
	cd apps; $(MAKE) clean
	rm -rf html

lib: FORCE
	cd lib; $(MAKE)

apps: FORCE
	cd apps; $(MAKE)

html/index.html: include/dbats.h
	rm -rf html
	doxygen

FORCE:
