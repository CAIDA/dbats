AC_REVISION([$Id: configure.ac,v 1.1 2013/09/11 23:52:27 kkeys Exp $])

AC_INIT(include/dbats.h)
AC_PREREQ(2.53)

AC_ARG_WITH(incdirs,
    [  --with-incdirs=DIRS     search for include files in DIRS])
AC_ARG_WITH(libdirs,
    [  --with-libdirs=DIRS     search for libraries in DIRS])
AC_ARG_WITH(inclibdirs,
    [  --with-inclibdirs=DIRS  same as --with-incdirs=DIRS --with-libdirs=DIRS])
AC_ARG_WITH(inclibprefix,
    [  --with-inclibprefix=DIR same as --with-incdirs=DIR/include --with-libdirs=DIR/lib])
AC_ARG_ENABLE(assert,
    [  --enable-assert         enable assert() for debugging],
    , enable_assert="default")
AC_ARG_ENABLE(developer, , ,
    test -r CVS && enable_developer=yes || enable_developer=no)

AC_CONFIG_HEADER(lib/config.h)

dnl MY_DIRS(src, dst[, prefix[, suffix]])
AC_DEFUN(MY_DIRS, [
    if test "${$1}" != "no" && test "${$1}" != ""; then
        for dir in ${$1}; do
            $2="${$2} $3${dir}$4"
        done
    fi
])

MY_DIRS(with_incdirs, CPPFLAGS, -I)
MY_DIRS(with_inclibdirs, CPPFLAGS, -I)
MY_DIRS(with_inclibprefix, CPPFLAGS, -I, /include)

LIBDIRS=""
MY_DIRS(with_libdirs, LIBDIRS, -L)
MY_DIRS(with_inclibdirs, LIBDIRS, -L)
MY_DIRS(with_inclibprefix, LIBDIRS, -L, /lib)

dnl Must append LIBDIRS to LDFLAGS now so it's present for AC_CHECK_LIB.
dnl (LIBDIRS will not work in the OTHER-LIBRARIES argument of AC_CHECK_LIB
dnl on all platforms since it will _follow_ the -l option for the library
dnl being tested.)
LDFLAGS="$LDFLAGS $LIBDIRS"

if test "${enable_assert}" = "default"; then
    if test "$enable_developer" = "yes"; then
        enable_assert="yes"
    else
        enable_assert="no"
    fi
fi
test "${enable_assert}" = "yes" || AC_DEFINE(NDEBUG, 1)

if test "${enable_developer}" = "yes" || test "${enable_caida}" = "yes"; then
#   Development/internal, default to CC=gcc-anal (if available).
    if test -z "${CC}"; then
        AC_CHECK_PROGS(GCC_ANAL, gcc-anal anal-gcc)
        if test -n "$GCC_ANAL"; then
            CC="$GCC_ANAL -pedantic"
        fi
    fi
fi
if test "${enable_developer}" = "yes"; then
    test -z "$CFLAGS" && CFLAGS='-g'
    #AC_MSG_RESULT([CFLAGS=$CFLAGS])
fi
AC_PROG_CC
if test "$GCC" = "yes"; then
    CC="$CC -std=gnu99"
fi


AC_CHECK_HEADERS(db.h)
#LIBS="-ldb $LIBS"

AC_RUN_IFELSE([AC_LANG_PROGRAM(
    [#include <db.h>],
    [exit(!(DB_VERSION_MAJOR >= 4));])],
    [db_version_ok=1], [db_version_ok=0], [db_version_ok=0])

if test "$db_version_ok" -ne "1"; then
    AC_MSG_ERROR([DB version 4 or higher is required.])
fi

AC_MSG_CHECKING([for DB->open()])
AC_LINK_IFELSE([AC_LANG_PROGRAM(
    [#include <db.h>],
    [DB *db; db->open(db,0,0,0,0,0,0);])],
    [AC_MSG_RESULT([yes]); AC_DEFINE(HAVE_DB_OPEN)],
    [AC_MSG_RESULT([no])])

AC_MSG_CHECKING([for DB_ENV->txn_begin()])
AC_LINK_IFELSE([AC_LANG_PROGRAM(
    [#include <db.h>],
    [DB_ENV *env; env->txn_begin(0,0,0,0);])],
    [AC_MSG_RESULT([yes]); AC_DEFINE(HAVE_DB_TXN_BEGIN)],
    [AC_MSG_RESULT([no])])

AC_MSG_CHECKING([for DB->set_lk_exclusive()])
AC_LINK_IFELSE([AC_LANG_PROGRAM(
    [#include <db.h>],
    [DB *db; db->set_lk_exclusive(db, 1);])],
    [AC_MSG_RESULT([yes]); AC_DEFINE(HAVE_DB_SET_LK_EXCLUSIVE)],
    [AC_MSG_RESULT([no])])

AC_MSG_CHECKING([for DB_TXN->set_priority()])
AC_LINK_IFELSE([AC_LANG_PROGRAM(
    [#include <db.h>],
    [DB_TXN *txn; txn->set_priority(txn, 100);])],
    [AC_MSG_RESULT([yes]); AC_DEFINE(HAVE_DB_SET_PRIORITY)],
    [AC_MSG_RESULT([no])])

AC_OUTPUT(lib/Makefile apps/Makefile)

AC_MSG_RESULT([])
AC_MSG_RESULT([Configuration complete.])

