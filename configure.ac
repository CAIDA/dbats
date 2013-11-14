AC_REVISION([$Id: configure.ac,v 1.25 2013/11/14 00:47:25 kkeys Exp $])

AC_PREREQ(2.68)
AC_INIT([DBATS], [0.1], [kkeys@caida.org])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR(include/dbats.h)
AC_CONFIG_HEADERS(lib/config.h)

dnl m4_include([m4/ax_pthread.m4]) dnl

AC_ARG_WITH(incdirs,
    [  --with-incdirs=DIRS     search for include files in DIRS])
AC_ARG_WITH(libdirs,
    [  --with-libdirs=DIRS     search for libraries in DIRS])
AC_ARG_WITH(inclibdirs,
    [  --with-inclibdirs=DIRS  same as --with-incdirs=DIRS --with-libdirs=DIRS])
AC_ARG_WITH(inclibprefix,
    [  --with-inclibprefix=DIR same as --with-incdirs=DIR/include --with-libdirs=DIR/lib])
AC_ARG_ENABLE(apache,
    [  --enable-apache         enable DBATS module for Apache httpd (requires
                          "make install-apache" to install)],
    , enable_apache=maybe)
AC_ARG_ENABLE(developer,
    [  --enable-developer      enable developer options],
    , test -r CVS && enable_developer=yes || enable_developer=no)
AC_ARG_ENABLE(assert,
    [  --enable-assert         enable assert() for debugging],
    , enable_assert="$enable_developer")


dnl Note: AM_INIT_AUTOMAKE may call AC_PROG_CC, so we must set CC first if we
dnl want to change the default.
if test "${enable_developer}" = "yes"; then
    if test -z "$CC" && gcc-anal --version >/dev/null 2>&1; then
	CC="gcc-anal -pedantic"
    fi
    test -z "$CFLAGS" && CFLAGS='-g'
fi

AM_INIT_AUTOMAKE([-Wall -Werror foreign])


dnl MY_DIRS(src, dst[, prefix[, suffix[, separator]]])
AC_DEFUN([MY_DIRS], [
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


test "${enable_assert}" = "yes" || AC_DEFINE([NDEBUG], 1, [Define to disable assert()])

if test "${enable_developer}" = "yes"; then
    AC_SUBST(DBATS_CORRUPT, [dbats_corrupt])
fi


# Checks for programs.

# note: AM_INIT_AUTOMAKE may or may not have already called AC_PROG_CC
AC_PROG_CC
AC_MSG_CHECKING([whether "$CC" accepts the "-std=c99" option])
if $CC -std=c99 --version >/dev/null 2>&1; then
    AC_MSG_RESULT([yes])
    CC="$CC -std=c99"
else
    AC_MSG_RESULT([no])
fi

AM_PROG_AR
AM_MISSING_PROG([DOXYGEN], [doxygen])
AC_SUBST(DOXYGEN)
LT_INIT
AC_SUBST([LIBTOOL_DEPS])


if test "${enable_apache}" != "no"; then
    AC_CHECK_PROGS(APXS, apxs2 apxs)
    if test "${enable_developer}" = "yes" && test "$GCC" = "yes"; then
	AC_SUBST(APXS_DEBUG, ["-Wc,-Wall -Wc,-Wshadow -Wc,-Wpointer-arith -Wc,-Wcast-align -Wc,-Wstrict-prototypes -Wc,-Wmissing-prototypes -Wc,-Wwrite-strings -Wc,-Winline -Wc,-Waggregate-return"])
    fi
    MSG="can't build apache module without apxs"
    if test -n "${APXS}"; then
	AC_SUBST(MOD_DBATS, mod_dbats.la)
    elif test "${enable_apache}" = "yes"; then
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

abs_srcdir=`( cd "$srcdir"; pwd; )`; # absolute directory
CPPFLAGS="-I${abs_srcdir}/include ${CPPFLAGS}"; # for uint.h during configure and make

POSIX_HEADER="
#define _POSIX_C_SOURCE 200809L
#include <unistd.h>"

DB_HEADER="
#include \"uint.h\"
#include <db.h>"

AC_CHECK_HEADERS(db.h, , , [
$POSIX_HEADER
#include "uint.h"
])


# Checks for typedefs, structures, and compiler characteristics.
AC_C_BIGENDIAN()
AC_TYPE_MODE_T
AC_TYPE_SIZE_T


# Checks for library functions.
AC_CHECK_FUNCS([fchmod])


# Other.

AC_MSG_CHECKING([Berkeley DB version])
# Check version in <db.h> against our minimum requirements (linking with libdb
# is not needed for this test).
AC_RUN_IFELSE([AC_LANG_PROGRAM([
    $POSIX_HEADER
    #include <stdlib.h>
    #include <errno.h>
    $DB_HEADER
    ], [
    FILE *out = fopen("conftest.out", "w");
    if (!out) exit(errno);
    fprintf(out, "dbh_major=%d\ndbh_minor=%d\ndbh_patch=%d\n",
	DB_VERSION_MAJOR, DB_VERSION_MINOR, DB_VERSION_PATCH);
    exit(0);
    ])],
    [], [AC_MSG_FAILURE([can not run compiled program])])
. ./conftest.out
dbh_version="$dbh_major.$dbh_minor.$dbh_patch"
AC_MSG_RESULT([$dbh_major.$dbh_minor.$dbh_patch])
if test "$dbh_major" -lt 4; then
    AC_MSG_ERROR([DB version 4 or higher is required.])
fi
if test "$dbh_major" -lt 5 || { test "$dbh_major" -eq 5 && test "$dbh_minor" -lt 3; } then
    AC_MSG_WARN([DB version 5.3 or higher is recommended.])
fi

dnl DBATS_CHECK_DB_FUNC(symbol, label, code, [action-if-not-found])
AC_DEFUN([DBATS_CHECK_DB_FUNC], [
AC_DEFINE($1, [0], [Define to 1 if you have $2])
AC_MSG_CHECKING([for $2])
AC_LINK_IFELSE([AC_LANG_PROGRAM([$POSIX_HEADER $DB_HEADER], $3)],
    [AC_MSG_RESULT([yes]); AC_DEFINE($1, [1])],
    [AC_MSG_RESULT([no]); $4])
])

DBATS_CHECK_DB_FUNC([HAVE_DB_TXN_BEGIN], [DB_ENV->txn_begin()],
    [DB_ENV *env=0; env->txn_begin(0,0,0,0);])

DBATS_CHECK_DB_FUNC([HAVE_DB_SET_LK_EXCLUSIVE], [DB->set_lk_exclusive()],
    [DB *db=0; db->set_lk_exclusive(db, 1);],
    [AC_MSG_WARN([Exclusive locking will not be available.])])

DBATS_CHECK_DB_FUNC([HAVE_DB_SET_PRIORITY], [DB_TXN->set_priority()],
    [DB_TXN *txn=0; txn->set_priority(txn, 100);])


# Output.
AC_CONFIG_FILES(Makefile include/Makefile lib/Makefile apps/Makefile tests/Makefile)
AC_OUTPUT()

AC_MSG_RESULT([])
AC_MSG_RESULT([Configuration complete.])

