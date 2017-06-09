#!/bin/bash
logPath="/data/achive/tmppy/*.log";
outPath="/data/achive/sql/$(date +\%Y\%m\%d_\%H\%M\%S).sql";
/bin/mv /data/log/*.log /data/achive/tmp
/bin/cat /data/achive/tmp/*.log > /data/achive/$(date +\%Y\%m\%d_\%H\%M\%S).log
/bin/gzip -c  /data/achive/tmp/*.log > /data/achive/package/log$(date +\%Y\%m\%d_\%H\%M\%S).log.gz
rm -rf /data/achive/tmp/*.log
/bin/mv /data/achive/*.log /data/achive/tmppy 
/usr/bin/python /data/tdagent/script/logisam.py $logPath $outPath 
/bin/gzip -c /data/achive/tmppy/*.log > /data/achive/package/catlog$(date +\%Y\%m\%d_\%H\%M\%S).log.gz
/bin/rm -rf /data/achive/tmppy/*.log
#/usr/bin/mysql -ulogdb -plog123456 -h10.10.91.175 logdb < /data/achive/sql/process.sql
/data/shells/mysqlsource.sh
/bin/gzip -c /data/achive/sql/*.sql > /data/achive/package/sql$(date +\%Y\%m\%d_\%H\%M\%S).sql.gz
rm -rf /data/achive/sql/*.sql
echo $(date +\%Y\%m\%d_\%H\%M\%S) PROCESS SUCCESSFUL!
