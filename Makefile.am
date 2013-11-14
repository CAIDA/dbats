## Process this file with automake to produce Makefile.in

SUBDIRS         = lib include apps . tests
EXTRA_DIST      = html Doxyfile
ACLOCAL_AMFLAGS = -I m4

all-local: libtool html/index.html

libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status libtool

# Generate html/ only if this is not a VPATH build (if it is, $srcdir/html/
# should already exist, and Doxyfile can't handle it anyway).
html html/index.html:
	if test "$(srcdir)" = "$(builddir)"; then \
	    rm -rf html; \
	    $(DOXYGEN) $(top_srcdir)/Doxyfile; \
	fi

install-data-hook:
	@if test -n "$(MOD_DBATS)"; then \
	    echo "#=================================================================="; \
	    echo "# Use 'make install-apache' to install the mod_dbats Apache module."; \
	    echo "#=================================================================="; \
	fi

install-apache: Makefile
	cd lib; $(MAKE) install-apache

distclean-local:
	-rm -rf html

# Replace "missing" with "missing-fail" to detect dependence on maintaner tools.
distcheck-hook:
	@chmod u+w $(distdir)
	-rm -f $(distdir)/missing
	cp missing-fail $(distdir)/missing
	@chmod u-w $(distdir)/missing $(distdir)
