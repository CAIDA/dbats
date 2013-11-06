## Process this file with automake to produce Makefile.in

# htmldir = $(datadir)/html
# html_DATA = html

SUBDIRS = lib include apps

ACLOCAL_AMFLAGS = -I m4

#libtool: $(LIBTOOL_DEPS)
#	$(SHELL) ./config.status libtool

############################################

#all: lib html/index.html apps 
#
#.PHONY: FORCE lib apps
#
#clean: FORCE
#	@#echo Making clean
#	cd lib; $(MAKE) clean
#	cd apps; $(MAKE) clean
#	rm -rf html
#
#lib: FORCE
#	@#echo Making lib
#	cd lib; $(MAKE)
#
#apps: FORCE
#	@#echo Making apps
#	cd apps; $(MAKE)
#
#install: FORCE
#	@#echo Installing DBATS Apache module
#	cd lib; $(MAKE) install
#
#html/index.html: include/dbats.h
#	rm -rf html
#	doxygen
#
#FORCE:
#	@#echo Making FORCE
#
