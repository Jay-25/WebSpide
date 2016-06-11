@ECHO OFF
chcp 936

title=%CD%

set APP=../WebSpideServer.jar
set APP_PARAS=-c E:\Homework\Ing\WebSpide\ini\House_webspide.ini -force
::set APP_PARAS=-c E:\Homework\Ing\WebSpide\ini\House_webspide.ini -keep -once

::set Jconsole_OPTS=-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false
set Code_OPTS=-Duser.country=ES -Duser.language=es -Duser.variant=Traditional_WIN -Dfile.encoding=UTF-8
set JAVA_OPTS=-Xverify:none -Xms1024M -Xmx1024M -Xmn600M -Xss1M -XX:ParallelGCThreads=2 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC  -XX:CMSInitiatingOccupancyFraction=85 -XX:MaxTenuringThreshold=0

:REDO
if exist %APP% (java %JAVA_OPTS% %Code_OPTS% %Jconsole_OPTS% -jar %APP% %APP_PARAS% %*) else echo %APP% not exit! & exit

set tmp=%date:~0,4%%date:~5,2%%date:~8,2%%time:~0,2%%time:~3,2%%time:~6,2%.tmp
echo "%APP_PARAS%" | find /c "-once" > %tmp%
for /f %%i in (%tmp%) do if %%i EQU 0 (del %tmp% & goto REDO)
del %tmp%
