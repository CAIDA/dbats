rm -rf test.dbats >/dev/null 2>&1
../apps/dbats_init -v10 -k test.keys test.dbats min:10 max:10 avg:10 last:10 sum:10
