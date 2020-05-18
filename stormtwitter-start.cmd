@echo off 
setlocal enabledelayedexpansion
start "stormtwitter" cmd.exe /K ""C:\Program Files\Java\jdk1.8.0_241\bin\java.exe" "-javaagent:C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2019.3.4\lib\idea_rt.jar=50183:C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2019.3.4\bin" -Dfile.encoding=UTF-8 -classpath C:\Users\user\AppData\Local\Temp\classpath1887231308.jar com.example.StormTwitter"
endlocal 