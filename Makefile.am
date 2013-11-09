## Process this file with automake to produce Makefile.in

# htmldir = $(datadir)/html
# html_DATA = html

SUBDIRS = . lib include apps

ACLOCAL_AMFLAGS = -I m4

all-local: libtool html/index.html

libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status libtool

html/index.html: include/dbats.h
	rm -rf html
	doxygen
