rm -rf test.dbats >/dev/null 2>&1
../apps/report-in -v10 -i -a5 test.dbats <${srcdir}/test.data
