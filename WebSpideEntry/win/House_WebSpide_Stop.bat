@ECHO OFF
chcp 936

title=%CD%

set APP=../WebSpide.jar
set APP_PARAS=-c E:\Homework\Ing\WebSpide\ini\House_webspide.ini -stop

set Code_OPTS=-Duser.country=ES -Duser.language=es -Duser.variant=Traditional_WIN -Dfile.encoding=UTF-8
set JAVA_OPTS=-Xverify:none -Xms1024M -Xmx1024M -Xmn600M -Xss1M -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC  -XX:CMSInitiatingOccupancyFraction=85 -XX:MaxTenuringThreshold=0

java %JAVA_OPTS% %Code_OPTS% -jar %APP% %APP_PARAS% %*
