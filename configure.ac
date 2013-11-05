AC_REVISION([$Id: configure.ac,v 1.15 2013/11/05 01:25:38 kkeys Exp $])

AC_PREREQ(2.68)
AC_INIT(DBATS, 0.1, kkeys@caida.org)
AC_CONFIG_SRCDIR(include/dbats.h)
AC_CONFIG_HEADERS(lib/config.h)

m4_include([m4/ax_pthread.m4]) dnl

AC_ARG_WITH(incdirs,
    [  --with-incdirs=DIRS     search for include files in DIRS])
AC_ARG_WITH(libdirs,
    [  --with-libdirs=DIRS     search for libraries in DIRS])
AC_ARG_WITH(inclibdirs,
    [  --with-inclibdirs=DIRS  same as --with-incdirs=DIRS --with-libdirs=DIRS])
AC_ARG_WITH(inclibprefix,
    [  --with-inclibprefix=DIR same as --with-incdirs=DIR/include --with-libdirs=DIR/lib])
AC_ARG_ENABLE(apache,
    [  --enable-apache         enable DBATS module for Apache httpd],
    , enable_apache=maybe)
AC_ARG_ENABLE(developer,
    [  --enable-developer      enable developer options],
    , test -r CVS && enable_developer=yes || enable_developer=no)
AC_ARG_ENABLE(assert,
    [  --enable-assert         enable assert() for debugging],
    , enable_assert="$enable_developer")

dnl MY_DIRS(src, dst[, prefix[, suffix[, separator]]])
AC_DEFUN(MY_DIRS, [
    sep="$5"
    if test "${$1}" != "no" && test "${$1}" != ""; then
        for dir in ${$1}; do
	    if test "${$2}" = ""; then
		$2="$3${dir}$4"
	    else
		$2="${$2}${sep:- }$3${dir}$4"
	    fi
        done
    fi
])

MY_DIRS(with_incdirs, CPPFLAGS, -I)
MY_DIRS(with_inclibdirs, CPPFLAGS, -I)
MY_DIRS(with_inclibprefix, CPPFLAGS, -I, /include)

MY_DIRS(with_libdirs, LDFLAGS, -L)
MY_DIRS(with_inclibdirs, LDFLAGS, -L)
MY_DIRS(with_inclibprefix, LDFLAGS, -L, /lib)

MY_DIRS(with_libdirs, LD_RUN_PATH, , , [:])
MY_DIRS(with_inclibdirs, LD_RUN_PATH, , , [:])
MY_DIRS(with_inclibprefix, LD_RUN_PATH, , /lib, [:])
AC_SUBST(LD_RUN_PATH)
export LD_RUN_PATH


test "${enable_assert}" = "yes" || AC_DEFINE(NDEBUG, 1)

# Checks for programs.
if test "${enable_developer}" = "yes"; then
    AC_PROG_CC(gcc-anal anal-gcc)
    if test "$GCC" = "yes"; then
	CC="$CC -pedantic"
    fi
    test -z "$CFLAGS" && CFLAGS='-g'
else
    AC_PROG_CC
fi

if test "$GCC" = "yes"; then
    CC="$CC -std=c99"
fi


if test "${enable_apache}" != "no"; then
    AC_CHECK_PROGS(APXS, apxs2 apxs)
    if test "${enable_developer}" = "yes" && test "$GCC" = "yes"; then
	AC_SUBST(APXS_DEBUG, ["-Wc,-Wall -Wc,-Wshadow -Wc,-Wpointer-arith -Wc,-Wcast-align -Wc,-Wstrict-prototypes -Wc,-Wmissing-prototypes -Wc,-Wwrite-strings -Wc,-Winline -Wc,-Waggregate-return"])
    fi

    MSG="can't build apache module without apxs"
    if test "${APXS}" != ""; then
	AC_SUBST(MOD_DBATS, mod_dbats.la)
	AC_SUBST(LIBTOOL, [$($APXS -q LIBTOOL)])
    elif test "${enable_apache}" == "yes"; then
	AC_MSG_ERROR([$MSG])
    else
	AC_MSG_WARN([$MSG])
    fi
fi


# Checks for libraries.
AC_CHECK_LIB([db], [db_version], , [
    # Some instances of libdb depend on pthreads, so try that.
    AX_PTHREAD()
    LIBS="$PTHREAD_LIBS $LIBS"
    CFLAGS="$CFLAGS $PTHREAD_CFLAGS"
    CC="$PTHREAD_CC"
    AC_CHECK_LIB([db], [db_version])
])


# Checks for header files.

POSIX_HEADER="
#define _POSIX_C_SOURCE 200809L
#include <unistd.h>"

DB_HEADER="
#include \"include/uint.h\"
#include <db.h>"

AC_CHECK_HEADERS(db.h, , , [
$POSIX_HEADER
#include "include/uint.h"
])


# Checks for typedefs, structures, and compiler characteristics.
AC_C_BIGENDIAN()


# Checks for library functions.
AC_CHECK_FUNCS([fchmod])




# Other.

AC_MSG_CHECKING([for dynamic linking options])
# Also, generate the conftest.out script used by the db version test below.
ORIG_LDFLAGS=$LDFLAGS
for opt in "" "-Wl,-rpath=" "-rpath " "-R "; do
    RPATH_OPTS=""
    if test -n "$opt"; then
	MY_DIRS(with_libdirs, RPATH_OPTS, $opt)
	MY_DIRS(with_inclibdirs, RPATH_OPTS, $opt)
	MY_DIRS(with_inclibprefix, RPATH_OPTS, $opt, /lib)
    fi
    LDFLAGS="$ORIG_LDFLAGS $RPATH_OPTS"
    AC_RUN_IFELSE([AC_LANG_PROGRAM([
	$POSIX_HEADER
	#include <stdlib.h>
	#include <errno.h>
	$DB_HEADER
	], [
	FILE *out = fopen("conftest.out", "w");
	if (!out) exit(errno);
	int major, minor, patch;
	db_version(&major,&minor,&patch);
	fprintf(out, "dbhversion=%d.%d.%d\ndbmajor=%d\ndbminor=%d\ndbpatch=%d\n",
	    DB_VERSION_MAJOR, DB_VERSION_MINOR, DB_VERSION_PATCH,
	    major, minor, patch);
	exit(0);
	])],
	[ok=1], [ok=0])
    if test $ok -eq 1; then
	AC_MSG_RESULT([${RPATH_OPTS:-(none needed)}])
	break
    fi
done
if test $ok -ne 1; then
    AC_MSG_RESULT([no working options found])
    AC_MSG_ERROR([You might want to try running configure again with CFLAGS=-static in the environment.])
fi

AC_MSG_CHECKING([Berkeley DB version])
. ./conftest.out
AC_MSG_RESULT([$dbmajor.$dbminor.$dbpatch])
if test "$dbmajor" -lt 4; then
    AC_MSG_ERROR([DB version 4 or higher is required.])
fi
if test "$dbmajor" -lt 5 || { test "$dbmajor" -eq 5 && test "$dbminor" -lt 3; } then
    AC_MSG_WARN([DB version 5.3 or higher is recommended.])
fi
if test "$dbhversion" != "$dbmajor.$dbminor.$dbpatch"; then
    AC_MSG_ERROR([DB_VERSION ($dbhversion) in <db.h> does not match db_version() ($dbmajor.$dbminor.$dbpatch) in libdb.])
fi

AC_MSG_CHECKING([for DB_ENV->txn_begin()])
AC_LINK_IFELSE([AC_LANG_PROGRAM(
    [$POSIX_HEADER $DB_HEADER],
    [DB_ENV *env=0; env->txn_begin(0,0,0,0);])],
    [AC_MSG_RESULT([yes]); AC_DEFINE(HAVE_DB_TXN_BEGIN)],
    [AC_MSG_RESULT([no])])

AC_MSG_CHECKING([for DB->set_lk_exclusive()])
AC_LINK_IFELSE([AC_LANG_PROGRAM(
    [$POSIX_HEADER $DB_HEADER],
    [DB *db=0; db->set_lk_exclusive(db, 1);])],
    [AC_MSG_RESULT([yes]); AC_DEFINE(HAVE_DB_SET_LK_EXCLUSIVE)],
    [AC_MSG_RESULT([no]); AC_MSG_WARN([Exclusive locking will not be available.])])

AC_MSG_CHECKING([for DB_TXN->set_priority()])
AC_LINK_IFELSE([AC_LANG_PROGRAM(
    [$POSIX_HEADER $DB_HEADER],
    [DB_TXN *txn=0; txn->set_priority(txn, 100);])],
    [AC_MSG_RESULT([yes]); AC_DEFINE(HAVE_DB_SET_PRIORITY)],
    [AC_MSG_RESULT([no])])


# Output.
AC_CONFIG_FILES(lib/Makefile apps/Makefile)
AC_OUTPUT()

AC_MSG_RESULT([])
AC_MSG_RESULT([Configuration complete.])

