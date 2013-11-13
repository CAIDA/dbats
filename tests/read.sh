rm test.out >/dev/null 2>&1
../apps/report-out -v10 test.dbats >test.out || exit $?
cmp test.out ${srcdir}/expected.out
