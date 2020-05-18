1. start hadoop with on Administrator CMD: start-all.cmd
2. start kafka and storm in same cli: storm-start-all.cmd
3. start namenode health chrome shortcut or put this in browser: http://localhost:50070/dfshealth.html#tab-overview
go to utilities tab and click on 'Browse File System'
4. check on stormtwitter cli log for "DEBUG com.example.HDFSBolt  - Writing to HDFS after scoring"
5. refresh namenode health browse file system window to see csv files increase size as they accumulate tweets realtime
6. to run another instance of stormtwitter run on same cli: stormtwitter-start.cmd