## Process this file with automake to produce Makefile.in

# htmldir = $(datadir)/html
# html_DATA = html

SUBDIRS         = lib include apps .
EXTRA_DIST      = html

ACLOCAL_AMFLAGS = -I m4

all-local: libtool html/index.html

libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status libtool

html/index.html: include/dbats.h
	rm -rf html
	doxygen

install-data-hook:
	@if test -n "$(MOD_DBATS)"; then \
	    echo "#=================================================================="; \
	    echo "# Use 'make install-apache' to install the mod_dbats Apache module."; \
	    echo "#=================================================================="; \
	fi

install-apache: Makefile
	cd lib; $(MAKE) install-apache
