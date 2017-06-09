#/bin/sh
/usr/bin/wget http://mytest.ufile.ucloud.cn/udb-10.10.91.175.log -O /data/logs/udb_new.log
/usr/bin/diff /data/logs/udb_new.log /data/logs/udb_old.log | grep failed >> /data/logs/udbfailed.log
/bin/rm -rf /data/logs/udb_old.log
/bin/mv /data/logs/udb_new.log /data/logs/udb_old.log


