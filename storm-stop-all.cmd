@echo off 
setlocal enabledelayedexpansion
taskkill /F /fi "WindowTitle eq storm*" /T
Taskkill /F /FI "WINDOWTITLE eq kafka*" /T
timeout 5
Taskkill /F /FI "WINDOWTITLE eq KafkaTwitterProducer*" /T
Taskkill /F /FI "WINDOWTITLE eq zk-server*" /T
start "Apache" cmd.exe /K "stop-all.cmd"
taskkill /F /FI "WindowTitle eq  Administrator: *" /T
endlocal 