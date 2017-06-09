#!/bin/bash
logPath="/data/achive/*.log";
outPath="/data/achive/sql/$(date +\%Y\%m\%d_\%H\%M\%S).sql";
mysqlConf="{'host':'10.6.15.53','user':'logdb','password':'log123456','db':'logdb'}";
python /data/tdagent/script/logExport.py $logPath $outPath $mysqlConf
mysql -ulogdb -plog123456 -h10.6.15.53 logdb</data/achive/sql/*.sql
#python logUtil.py $logPath
