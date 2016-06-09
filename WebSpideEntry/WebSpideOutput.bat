@ECHO OFF
chcp 936

title=%CD%

set APP=WebSpideOutput.jar
set APP_PARAS=-c CaiShiChang_webspide.ini,YiMuTian_webspide.ini,House_webspide.ini

::set Jconsole_OPTS=-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false
set Code_OPTS=-Duser.country=ES -Duser.language=es -Duser.variant=Traditional_WIN -Dfile.encoding=UTF-8
set JAVA_OPTS=-Xverify:none -Xms1024M -Xmx1024M -Xmn600M -Xss1M -XX:ParallelGCThreads=2 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC  -XX:CMSInitiatingOccupancyFraction=85 -XX:MaxTenuringThreshold=0

E:
cd E:\Homework\Ing\WebSpide\WebSpideEntry\

:REDO
if exist %APP% (java %JAVA_OPTS% %Code_OPTS% %Jconsole_OPTS% -jar %APP% %APP_PARAS% %*) else echo %APP% not exit! & exit
goto REDO
