#!/bin/sh
export LANG=zh_CN.GBK

APP=../WebSpide.jar
APP_PARAS="-c ../ini/House_webspide.ini"

#Jconsole_OPTS="-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
Code_OPTS="-Duser.country=ES -Duser.language=es -Duser.variant=Traditional_WIN -Dfile.encoding=UTF-8"
JAVA_OPTS="-Xverify:none -Xms512M -Xmx512M -Xmn300M -Xss1M -XX:ParallelGCThreads=2 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC  -XX:CMSInitiatingOccupancyFraction=85 -XX:MaxTenuringThreshold=0"

while [ -f $APP ]
do
    java $JAVA_OPTS $Code_OPTS $Jconsole_OPTS -jar $APP $APP_PARAS
    let pos=`echo "$APP_PARAS" | awk -F "-once" '{printf "%d", length($0)-length($NF)}'`
    if [[ $pos -ne 0 ]];then
       break
    fi
done
