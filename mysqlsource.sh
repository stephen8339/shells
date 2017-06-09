#!/bin/bash
#set -e
LC_ALL=C
LANG=C
unset TZ
TZBase=$(LC_ALL=C TZ=UTC0 date -R)
UTdate=$(LC_ALL=C TZ=UTC0 date -d "$TZBase")
TZdate=$(unset TZ ; LANG=C date -d "$TZBase")
file_path="/data/achive/sql/"              #要导入的sql文件夹
host="datadb.czozwo9daiag.ap-northeast-1.rds.amazonaws.com"                  #要导入的mysql主机
username="logdb"                      #mysql的用户名
password="log123456"              #mysql的密码
dbname="logdb"                        #mysql的数据库名
now=$(date "+%s")                      #计时

mysql_source()
{
        for file_name in `ls -A $1`
        do
                seg_start_time=$(date "+%s")
                if [ -f "$1$file_name" ];
                then
                        command="source $1$file_name"
                        mysql -h${host} -u${username} -p${password} ${dbname} -e "$command"
                        echo "source:" /"$1$file_name/" "is ok, It takes " `expr $(date "+%s") - ${seg_start_time}` " seconds"
                fi
        done
echo "All sql is done! Total cost: " `expr $(date "+%s") - ${now}` " seconds"
}

mysql_connect_test()
{
        commandtest="show databases;"
        mysql -h${host} -u${username} -p${password} -e "$commandtest" > /dev/null 2>&1
}

mysql_test()
{
        mysql_connect_test
        if [ $? == 0 ]
        then
                echo "$host mysql connected"
                echo "Universal Time is now:  $UTdate."
                echo "Local time is now:      $TZdate."
                mysql_source $file_path
                exit 0
        else
                echo "[ERROR] $host mysql connect failed,retrying in 10s..."
                sleep 10
                mysql_test
                exit 2
        fi
}
mysql_test
