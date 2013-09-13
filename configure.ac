AC_REVISION([$Id: configure.ac,v 1.5 2013/09/13 21:16:02 kkeys Exp $])

AC_INIT(include/dbats.h)
AC_PREREQ(2.53)

m4_include([m4/ax_pthread.m4]) dnl

AC_ARG_WITH(incdirs,
    [  --with-incdirs=DIRS     search for include files in DIRS])
AC_ARG_WITH(libdirs,
    [  --with-libdirs=DIRS     search for libraries in DIRS])
AC_ARG_WITH(inclibdirs,
    [  --with-inclibdirs=DIRS  same as --with-incdirs=DIRS --with-libdirs=DIRS])
AC_ARG_WITH(inclibprefix,
    [  --with-inclibprefix=DIR same as --with-incdirs=DIR/include --with-libdirs=DIR/lib])
AC_ARG_ENABLE(developer,
    [  --enable-developer      enable developer options],
    , test -r CVS && enable_developer=yes || enable_developer=no)
AC_ARG_ENABLE(assert,
    [  --enable-assert         enable assert() for debugging],
    , enable_assert="$enable_developer")

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

MY_DIRS(with_libdirs, LDFLAGS, -L)
MY_DIRS(with_inclibdirs, LDFLAGS, -L)
MY_DIRS(with_inclibprefix, LDFLAGS, -L, /lib)


test "${enable_assert}" = "yes" || AC_DEFINE(NDEBUG, 1)

if test "${enable_developer}" = "yes"; then
#   default to CC=gcc-anal (if available).
    if test -z "${CC}"; then
        AC_CHECK_PROGS(GCC_ANAL, gcc-anal anal-gcc)
        if test -n "$GCC_ANAL"; then
            CC="$GCC_ANAL -pedantic"
        fi
    fi
    test -z "$CFLAGS" && CFLAGS='-g'
fi

AC_PROG_CC
if test "$GCC" = "yes"; then
    CC="$CC -std=gnu99"
fi


AC_CHECK_HEADERS(db.h)
LIBS="-ldb $LIBS"


PROGINC="
#include <stdlib.h>
#include <db.h>"
PROGBODY="
db_version(0,0,0); exit(0);"


# Simple db program to check linking with -ldb
AC_MSG_CHECKING([for linking with db (without pthreads)])
AC_LINK_IFELSE([AC_LANG_PROGRAM([$PROGINC],[$PROGBODY])],
    [AC_MSG_RESULT([yes])],
    [
	AC_MSG_RESULT([no])
	# try again with pthreads
	AX_PTHREAD()
	AC_MSG_CHECKING([for linking with db (with pthreads)])
	LIBS="$PTHREAD_LIBS $LIBS"
	CFLAGS="$CFLAGS $PTHREAD_CFLAGS"
	CC="$PTHREAD_CC"
	AC_LINK_IFELSE([AC_LANG_PROGRAM([$PROGINC],[$PROGBODY])],
	    [AC_MSG_RESULT([yes])],
	    [AC_MSG_RESULT([no]); AC_MSG_ERROR([unable to link with db])])
    ])

# If running the same program doesn't work, try re-linking with different
# RPATH options.
AC_MSG_CHECKING([for dynamic linking options])
ORIG_LDFLAGS=$LDFLAGS
for opt in "" "-Wl,-rpath=" "-rpath " "-R "; do
    RPATH_OPTS=""
    if test -n "$opt"; then
	for path in $with_libdirs $with_inclibdirs; do
	    RPATH_OPTS="$RPATH_OPTS $opt$path"
	done
	for path in $with_inclibprefix; do
	    RPATH_OPTS="$RPATH_OPTS $opt$path/lib"
	done
    fi
    LDFLAGS="$ORIG_LDFLAGS $RPATH_OPTS"
    AC_RUN_IFELSE([AC_LANG_PROGRAM([$PROGINC],[$PROGBODY])],
	[run_ok=1], [run_ok=0], [run_ok=0])
    if test $run_ok -eq 1; then
	AC_MSG_RESULT([${RPATH_OPTS:-(none needed)}])
	break
    fi
done
if test $run_ok -ne 1; then
    AC_MSG_RESULT([no working options found])
    AC_MSG_ERROR([You might want to try running configure again with CFLAGS=-static in the environment.])
fi

AC_MSG_CHECKING([Berkeley DB version])
AC_RUN_IFELSE([AC_LANG_PROGRAM([
    #include <stdlib.h>
    #include <errno.h>
    #include <db.h>
    ], [
    FILE *out = fopen("conftest.out", "w");
    if (!out) exit(errno);
    int major, minor, patch;
    db_version(&major,&minor,&patch);
    fprintf(out, "dbmajor=%d\ndbminor=%d\ndbpatch=%d\n", major, minor, patch);
    exit(0);
    ])],
    [ok=1], [ok=0], [ok=0])
if test "$ok" -ne "1"; then
    AC_MSG_ERROR([error])
fi
. ./conftest.out
AC_MSG_RESULT([$dbmajor.$dbminor.$dbpatch])
if test "$dbmajor" -lt 4; then
    AC_MSG_ERROR([DB version 4 or higher is required.])
fi
if test "$dbmajor" -lt 5 || { test "$dbmajor" -eq 5 && test "$dbminor" -lt 3; } then
    AC_MSG_WARN([DB version 5.3 or higher is recommended.])
fi

AC_RUN_IFELSE([AC_LANG_PROGRAM([
    #include <db.h>
    ], [
    fprintf(stderr, "db_version: %s\n", db_version(0,0,0));
    fprintf(stderr, "DB_VERSION_STRING: %s\n", DB_VERSION_STRING);
    exit(!!strcmp(db_version(0,0,0), DB_VERSION_STRING));
    ])],
    [db_version_ok=1], [db_version_ok=0], [db_version_ok=0])
if test "$db_version_ok" -ne "1"; then
    AC_MSG_ERROR([DB_VERSION_STRING in <db.h> does not match db_version() in library.])
fi

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

