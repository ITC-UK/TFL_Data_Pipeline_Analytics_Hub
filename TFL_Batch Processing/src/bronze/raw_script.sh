[Consultants@ip-172-31-8-235 TFL_Batch_Processing]$ cd
[Consultants@ip-172-31-8-235 ~]$ hdfs dfd -ls /tmp/DE011025
ERROR: dfd is not COMMAND nor fully qualified CLASSNAME.
Usage: hdfs [OPTIONS] SUBCOMMAND [SUBCOMMAND OPTIONS]

  OPTIONS is none or any of:

--buildpaths                       attempt to add class files from build tree
--config dir                       Hadoop config directory
--daemon (start|status|stop)       operate on a daemon
--debug                            turn on shell script debug mode
--help                             usage information
--hostnames list[,of,host,names]   hosts to use in worker mode
--hosts filename                   list of hosts to use in worker mode
--loglevel level                   set the log4j level for this command
--workers                          turn on worker mode

  SUBCOMMAND is one of:


    Admin Commands:

cacheadmin           configure the HDFS cache
crypto               configure HDFS encryption zones
debug                run a Debug Admin to execute HDFS debug commands
dfsadmin             run a DFS admin client
dfsrouteradmin       manage Router-based federation
ec                   run a HDFS ErasureCoding CLI
fsck                 run a DFS filesystem checking utility
fsImageValidation    run FsImageValidation to check an fsimage
haadmin              run a DFS HA admin client
jmxget               get JMX exported values from NameNode or DataNode.
oev                  apply the offline edits viewer to an edits file
oiv                  apply the offline fsimage viewer to an fsimage
oiv_legacy           apply the offline fsimage viewer to a legacy fsimage
storagepolicies      list/get/set block storage policies

    Client Commands:

classpath            prints the class path needed to get the hadoop jar and the required libraries
dfs                  run a filesystem command on the file system
envvars              display computed Hadoop environment variables
fetchdt              fetch a delegation token from the NameNode
getconf              get config values from configuration
groups               get the groups which users belong to
lsSnapshot           list all snapshots for a snapshottable directory
lsSnapshottableDir   list all snapshottable dirs owned by the current user
snapshotDiff         diff two snapshots of a directory or diff the current directory contents with a snapshot
version              print the version

    Daemon Commands:

balancer             run a cluster balancing utility
datanode             run a DFS datanode
dfsrouter            run the DFS router
diskbalancer         Distributes data evenly among disks on a given node
httpfs               run HttpFS server, the HDFS HTTP Gateway
journalnode          run the DFS journalnode
mover                run a utility to move block replicas across storage types
namenode             run the DFS namenode
nfs3                 run an NFS version 3 gateway
portmap              run a portmap service
secondarynamenode    run the DFS secondary namenode
zkfc                 run the ZK Failover Controller daemon

SUBCOMMAND may print help when invoked w/o parameters or with -h.
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025
Found 26 items
drwxr-xr-x   - Consultants hadoop          0 2025-11-25 11:19 /tmp/DE011025/Aamir
drwxr-xr-x   - hdfs        hadoop          0 2025-10-10 12:26 /tmp/DE011025/Alan
drwxr-xr-x   - Consultants hadoop          0 2025-11-19 16:33 /tmp/DE011025/AlanATL
drwxr-xr-x   - Consultants hadoop          0 2025-10-09 13:41 /tmp/DE011025/Amjad
drwxr-xr-x   - Consultants hadoop          0 2025-10-15 23:05 /tmp/DE011025/Consultants
drwxrwxr-x   - Consultants hadoop          0 2025-11-19 09:57 /tmp/DE011025/Gharza
drwxr-xr-x   - Consultants hadoop          0 2025-11-19 09:55 /tmp/DE011025/Gharzay
drwxr-xr-x   - Consultants hadoop          0 2025-11-26 22:48 /tmp/DE011025/Jay
drwxrwxrwx   - Consultants hadoop          0 2025-11-26 19:07 /tmp/DE011025/Joe
drwxr-xr-x   - Consultants hadoop          0 2025-10-13 18:17 /tmp/DE011025/Marc
drwxr-xr-x   - Consultants hadoop          0 2025-11-25 16:35 /tmp/DE011025/NBA
drwxr-xr-x   - Consultants hadoop          0 2025-10-20 00:34 /tmp/DE011025/Raji
drwxr-xr-x   - Consultants hadoop          0 2025-10-15 13:32 /tmp/DE011025/Smith
drwxr-xr-x   - Consultants hadoop          0 2025-11-10 17:18 /tmp/DE011025/Sylvester
drwxr-xr-x   - Consultants hadoop          0 2025-10-20 14:08 /tmp/DE011025/jay
-rw-r--r--   3 Consultants hadoop        765 2025-10-13 18:40 /tmp/DE011025/joe
drwxrwxrwx   - Consultants hadoop          0 2025-11-14 11:11 /tmp/DE011025/kavya
drwxr-xr-x   - Consultants hadoop          0 2025-11-25 13:15 /tmp/DE011025/lew
drwxr-xr-x   - Consultants hadoop          0 2025-10-21 09:58 /tmp/DE011025/lewtwelf
drwxr-xr-x   - Consultants hadoop          0 2025-10-16 17:32 /tmp/DE011025/marc
drwxr-xr-x   - Consultants hadoop          0 2025-10-09 13:38 /tmp/DE011025/nicksteen
drwxrwxrwx   - Consultants hadoop          0 2025-11-25 12:56 /tmp/DE011025/padmasri
drwxrwxrwx   - Consultants hive            0 2025-11-25 13:11 /tmp/DE011025/sanket
drwxr-xr-x   - Consultants hadoop          0 2025-11-26 12:17 /tmp/DE011025/stocksdata
drwxr-xr-x   - jenkins     hadoop          0 2025-11-27 15:06 /tmp/DE011025/uk
drwxrwxrwx   - Consultants hadoop          0 2025-11-10 15:32 /tmp/DE011025/uttam
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/uk
Found 1 items
drwxr-xr-x   - jenkins hadoop          0 2025-11-27 15:06 /tmp/DE011025/uk/streaming
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -mkdir /tmp/DE011025/TFL_Batch_processing
[Consultants@ip-172-31-8-235 ~]$ sqoop import --connect jdbc:postgresql://18.134.163.221:5432/testdb --username Consultants --passwor
d WelcomeItc@2022 --table TFL_bakerloo_lines --m 1 --target-dir /tmp/DE011025/TFL_Batch_processing/raw/ TFL_bakerloo_lines
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/log4j-slf4j-impl-2.13.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
25/11/27 20:10:11 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7.7.1.7.0-551
25/11/27 20:10:11 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
25/11/27 20:10:11 ERROR tool.BaseSqoopTool: Error parsing arguments for import:
25/11/27 20:10:11 ERROR tool.BaseSqoopTool: Unrecognized argument: TFL_bakerloo_lines

Try --help for usage instructions.
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing
[Consultants@ip-172-31-8-235 ~]$ sqoop import \
>  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
>  --username Consultants \
>  --password WelcomeItc@2022 \
o_lines>  --table TFL_bakerloo_lines \
>  --m 1 \
>  --target-dir /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/log4j-slf4j-impl-2.13.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
25/11/27 20:12:52 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7.7.1.7.0-551
25/11/27 20:12:52 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
25/11/27 20:12:52 INFO manager.SqlManager: Using default fetchSize of 1000
25/11/27 20:12:52 INFO tool.CodeGenTool: Beginning code generation
25/11/27 20:12:53 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM "TFL_bakerloo_lines" AS t LIMIT 1
25/11/27 20:12:53 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce
warning: No SupportedSourceVersion annotation found on org.apache.hadoop.hdds.conf.ConfigFileGenerator, returning RELEASE_6.
warning: Supported source version 'RELEASE_6' from annotation processor 'org.apache.hadoop.hdds.conf.ConfigFileGenerator' less than -source '1.8'
2 warnings
25/11/27 20:12:55 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-Consultants/compile/60a2d66d9a0776d313fd8582abe2d581/TFL_bakerloo_lines.jar
25/11/27 20:12:55 WARN manager.PostgresqlManager: It looks like you are importing from postgresql.
25/11/27 20:12:55 WARN manager.PostgresqlManager: This transfer can be faster! Use the --direct
25/11/27 20:12:55 WARN manager.PostgresqlManager: option to exercise a postgresql-specific fast path.
25/11/27 20:12:55 INFO mapreduce.ImportJobBase: Beginning import of TFL_bakerloo_lines
25/11/27 20:12:55 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
25/11/27 20:12:55 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
25/11/27 20:12:55 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-3-80.eu-west-2.compute.internal/172.31.3.80:8032
25/11/27 20:12:56 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/Consultants/.staging/job_1764063460733_0052
25/11/27 20:13:01 INFO db.DBInputFormat: Using read commited transaction isolation
25/11/27 20:13:01 INFO mapreduce.JobSubmitter: number of splits:1
25/11/27 20:13:01 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1764063460733_0052
25/11/27 20:13:01 INFO mapreduce.JobSubmitter: Executing with tokens: []
25/11/27 20:13:01 INFO conf.Configuration: resource-types.xml not found
25/11/27 20:13:01 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
25/11/27 20:13:01 INFO impl.YarnClientImpl: Submitted application application_1764063460733_0052
25/11/27 20:13:02 INFO mapreduce.Job: The url to track the job: http://ip-172-31-3-80.eu-west-2.compute.internal:8088/proxy/application_1764063460733_0052/
25/11/27 20:13:02 INFO mapreduce.Job: Running job: job_1764063460733_0052
25/11/27 20:13:34 INFO mapreduce.Job: Job job_1764063460733_0052 running in uber mode : false
25/11/27 20:13:34 INFO mapreduce.Job:  map 0% reduce 0%
25/11/27 20:13:51 INFO mapreduce.Job: Task Id : attempt_1764063460733_0052_m_000000_0, Status : FAILED
Error: java.lang.RuntimeException: org.postgresql.util.PSQLException: The connection attempt failed.
        at org.apache.sqoop.mapreduce.db.DBInputFormat.getConnection(DBInputFormat.java:226)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.setDbConf(DBInputFormat.java:170)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.setConf(DBInputFormat.java:160)
        at org.apache.hadoop.util.ReflectionUtils.setConf(ReflectionUtils.java:77)
        at org.apache.hadoop.util.ReflectionUtils.newInstance(ReflectionUtils.java:137)
        at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:763)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:347)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1898)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)
Caused by: org.postgresql.util.PSQLException: The connection attempt failed.
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:297)
        at org.postgresql.core.ConnectionFactory.openConnection(ConnectionFactory.java:49)
        at org.postgresql.jdbc.PgConnection.<init>(PgConnection.java:217)
        at org.postgresql.Driver.makeConnection(Driver.java:458)
        at org.postgresql.Driver.connect(Driver.java:260)
        at java.sql.DriverManager.getConnection(DriverManager.java:664)
        at java.sql.DriverManager.getConnection(DriverManager.java:247)
        at org.apache.sqoop.mapreduce.db.DBConfiguration.getConnection(DBConfiguration.java:300)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.getConnection(DBInputFormat.java:218)
        ... 11 more
Caused by: java.net.SocketTimeoutException: connect timed out
        at java.net.PlainSocketImpl.socketConnect(Native Method)
        at java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:350)
        at java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:206)
        at java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:188)
        at java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392)
        at java.net.Socket.connect(Socket.java:607)
        at org.postgresql.core.PGStream.<init>(PGStream.java:81)
        at org.postgresql.core.v3.ConnectionFactoryImpl.tryConnect(ConnectionFactoryImpl.java:93)
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:197)
        ... 19 more

25/11/27 20:14:05 INFO mapreduce.Job: Task Id : attempt_1764063460733_0052_m_000000_1, Status : FAILED
Error: java.lang.RuntimeException: org.postgresql.util.PSQLException: The connection attempt failed.
        at org.apache.sqoop.mapreduce.db.DBInputFormat.getConnection(DBInputFormat.java:226)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.setDbConf(DBInputFormat.java:170)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.setConf(DBInputFormat.java:160)
        at org.apache.hadoop.util.ReflectionUtils.setConf(ReflectionUtils.java:77)
        at org.apache.hadoop.util.ReflectionUtils.newInstance(ReflectionUtils.java:137)
        at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:763)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:347)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1898)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)
Caused by: org.postgresql.util.PSQLException: The connection attempt failed.
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:297)
        at org.postgresql.core.ConnectionFactory.openConnection(ConnectionFactory.java:49)
        at org.postgresql.jdbc.PgConnection.<init>(PgConnection.java:217)
        at org.postgresql.Driver.makeConnection(Driver.java:458)
        at org.postgresql.Driver.connect(Driver.java:260)
        at java.sql.DriverManager.getConnection(DriverManager.java:664)
        at java.sql.DriverManager.getConnection(DriverManager.java:247)
        at org.apache.sqoop.mapreduce.db.DBConfiguration.getConnection(DBConfiguration.java:300)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.getConnection(DBInputFormat.java:218)
        ... 11 more
Caused by: java.net.SocketTimeoutException: connect timed out
        at java.net.PlainSocketImpl.socketConnect(Native Method)
        at java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:350)
        at java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:206)
        at java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:188)
        at java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392)
        at java.net.Socket.connect(Socket.java:607)
        at org.postgresql.core.PGStream.<init>(PGStream.java:81)
        at org.postgresql.core.v3.ConnectionFactoryImpl.tryConnect(ConnectionFactoryImpl.java:93)
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:197)
        ... 19 more

25/11/27 20:14:18 INFO mapreduce.Job: Task Id : attempt_1764063460733_0052_m_000000_2, Status : FAILED
Error: java.lang.RuntimeException: org.postgresql.util.PSQLException: The connection attempt failed.
        at org.apache.sqoop.mapreduce.db.DBInputFormat.getConnection(DBInputFormat.java:226)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.setDbConf(DBInputFormat.java:170)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.setConf(DBInputFormat.java:160)
        at org.apache.hadoop.util.ReflectionUtils.setConf(ReflectionUtils.java:77)
        at org.apache.hadoop.util.ReflectionUtils.newInstance(ReflectionUtils.java:137)
        at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:763)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:347)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1898)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)
Caused by: org.postgresql.util.PSQLException: The connection attempt failed.
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:297)
        at org.postgresql.core.ConnectionFactory.openConnection(ConnectionFactory.java:49)
        at org.postgresql.jdbc.PgConnection.<init>(PgConnection.java:217)
        at org.postgresql.Driver.makeConnection(Driver.java:458)
        at org.postgresql.Driver.connect(Driver.java:260)
        at java.sql.DriverManager.getConnection(DriverManager.java:664)
        at java.sql.DriverManager.getConnection(DriverManager.java:247)
        at org.apache.sqoop.mapreduce.db.DBConfiguration.getConnection(DBConfiguration.java:300)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.getConnection(DBInputFormat.java:218)
        ... 11 more
Caused by: java.net.SocketTimeoutException: connect timed out
        at java.net.PlainSocketImpl.socketConnect(Native Method)
        at java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:350)
        at java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:206)
        at java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:188)
        at java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392)
        at java.net.Socket.connect(Socket.java:607)
        at org.postgresql.core.PGStream.<init>(PGStream.java:81)
        at org.postgresql.core.v3.ConnectionFactoryImpl.tryConnect(ConnectionFactoryImpl.java:93)
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:197)
        ... 19 more

25/11/27 20:14:26 INFO mapreduce.Job:  map 100% reduce 0%
25/11/27 20:14:27 INFO mapreduce.Job: Job job_1764063460733_0052 completed successfully
25/11/27 20:14:27 INFO mapreduce.Job: Counters: 34
        File System Counters
                FILE: Number of bytes read=0
                FILE: Number of bytes written=260328
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=85
                HDFS: Number of bytes written=1000701
                HDFS: Number of read operations=6
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Failed map tasks=3
                Launched map tasks=4
                Other local map tasks=4
                Total time spent by all maps in occupied slots (ms)=43223
                Total time spent by all reduces in occupied slots (ms)=0
                Total time spent by all map tasks (ms)=43223
                Total vcore-milliseconds taken by all map tasks=172892
                Total megabyte-milliseconds taken by all map tasks=354082816
        Map-Reduce Framework
                Map input records=1731
                Map output records=1731
                Input split bytes=85
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=45
                CPU time spent (ms)=2030
                Physical memory (bytes) snapshot=361136128
                Virtual memory (bytes) snapshot=6655881216
                Total committed heap usage (bytes)=348127232
                Peak Map Physical memory (bytes)=361136128
                Peak Map Virtual memory (bytes)=6655881216
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=1000701
25/11/27 20:14:27 INFO mapreduce.ImportJobBase: Transferred 977.2471 KB in 91.5129 seconds (10.6788 KB/sec)
25/11/27 20:14:27 INFO mapreduce.ImportJobBase: Retrieved 1731 records.
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw/
Found 1 items
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:14 /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines
Found 2 items
-rw-r--r--   3 Consultants hadoop          0 2025-11-27 20:14 /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines/_SUCCESS     
-rw-r--r--   3 Consultants hadoop    1000701 2025-11-27 20:14 /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines/part-m-00000 
[Consultants@ip-172-31-8-235 ~]$ sqoop import \
.221:54>  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
>  --username Consultants \
>  --password WelcomeItc@2022 \
>  --table TFL_central_lines\
>  --m 1 \
>  --target-dir /tmp/DE011025/TFL_Batch_processing/raw/ TFL_central_lines
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/log4j-slf4j-impl-2.13.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
25/11/27 20:25:37 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7.7.1.7.0-551
25/11/27 20:25:37 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
25/11/27 20:25:37 ERROR tool.BaseSqoopTool: Error parsing arguments for import:
25/11/27 20:25:37 ERROR tool.BaseSqoopTool: Unrecognized argument: TFL_central_lines

Try --help for usage instructions.
[Consultants@ip-172-31-8-235 ~]$ sqoop import  --connect jdbc:postgresql://18.134.163.221:5432/testdb  --username Consultants  --password WelcomeItc@2022  --table TFL_central_lines --m 1  --target-dir /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/log4j-slf4j-impl-2.13.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
25/11/27 20:25:57 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7.7.1.7.0-551
25/11/27 20:25:57 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
25/11/27 20:25:57 INFO manager.SqlManager: Using default fetchSize of 1000
25/11/27 20:25:57 INFO tool.CodeGenTool: Beginning code generation
25/11/27 20:25:57 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM "TFL_central_lines" AS t LIMIT 1
25/11/27 20:25:57 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce
warning: No SupportedSourceVersion annotation found on org.apache.hadoop.hdds.conf.ConfigFileGenerator, returning RELEASE_6.
warning: Supported source version 'RELEASE_6' from annotation processor 'org.apache.hadoop.hdds.conf.ConfigFileGenerator' less than -source '1.8'
2 warnings
25/11/27 20:25:59 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-Consultants/compile/f8fba10c1809350f4340fd52ebe49092/TFL_central_lines.jar
25/11/27 20:25:59 WARN manager.PostgresqlManager: It looks like you are importing from postgresql.
25/11/27 20:25:59 WARN manager.PostgresqlManager: This transfer can be faster! Use the --direct
25/11/27 20:25:59 WARN manager.PostgresqlManager: option to exercise a postgresql-specific fast path.
25/11/27 20:25:59 INFO mapreduce.ImportJobBase: Beginning import of TFL_central_lines
25/11/27 20:25:59 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
25/11/27 20:26:00 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
25/11/27 20:26:00 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-3-80.eu-west-2.compute.internal/172.31.3.80:8032
25/11/27 20:26:01 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/Consultants/.staging/job_1764063460733_0053
25/11/27 20:26:07 INFO db.DBInputFormat: Using read commited transaction isolation
25/11/27 20:26:07 INFO mapreduce.JobSubmitter: number of splits:1
25/11/27 20:26:07 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1764063460733_0053
25/11/27 20:26:07 INFO mapreduce.JobSubmitter: Executing with tokens: []
25/11/27 20:26:08 INFO conf.Configuration: resource-types.xml not found
25/11/27 20:26:08 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
25/11/27 20:26:08 INFO impl.YarnClientImpl: Submitted application application_1764063460733_0053
25/11/27 20:26:08 INFO mapreduce.Job: The url to track the job: http://ip-172-31-3-80.eu-west-2.compute.internal:8088/proxy/application_1764063460733_0053/
25/11/27 20:26:08 INFO mapreduce.Job: Running job: job_1764063460733_0053
25/11/27 20:26:14 INFO mapreduce.Job: Job job_1764063460733_0053 running in uber mode : false
25/11/27 20:26:14 INFO mapreduce.Job:  map 0% reduce 0%
25/11/27 20:26:35 INFO mapreduce.Job:  map 100% reduce 0%
25/11/27 20:26:36 INFO mapreduce.Job: Job job_1764063460733_0053 completed successfully
25/11/27 20:26:36 INFO mapreduce.Job: Counters: 33
        File System Counters
                FILE: Number of bytes read=0
                FILE: Number of bytes written=260324
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=85
                HDFS: Number of bytes written=866289
                HDFS: Number of read operations=6
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=1
                Other local map tasks=1
                Total time spent by all maps in occupied slots (ms)=18946
                Total time spent by all reduces in occupied slots (ms)=0
                Total time spent by all map tasks (ms)=18946
                Total vcore-milliseconds taken by all map tasks=75784
                Total megabyte-milliseconds taken by all map tasks=155205632
        Map-Reduce Framework
                Map input records=1547
                Map output records=1547
                Input split bytes=85
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=322
                CPU time spent (ms)=5780
                Physical memory (bytes) snapshot=284569600
                Virtual memory (bytes) snapshot=6627102720
                Total committed heap usage (bytes)=221773824
                Peak Map Physical memory (bytes)=284569600
                Peak Map Virtual memory (bytes)=6627102720
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=866289
25/11/27 20:26:36 INFO mapreduce.ImportJobBase: Transferred 845.9854 KB in 36.2962 seconds (23.3079 KB/sec)
25/11/27 20:26:36 INFO mapreduce.ImportJobBase: Retrieved 1547 records.
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines
Found 2 items
-rw-r--r--   3 Consultants hadoop          0 2025-11-27 20:26 /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines/_SUCCESS      
-rw-r--r--   3 Consultants hadoop     866289 2025-11-27 20:26 /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines/part-m-00000  
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw
Found 2 items
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:14 /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:26 /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines
[Consultants@ip-172-31-8-235 ~]$ sqoop import \
>  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
>  --username Consultants \
>  --password WelcomeItc@2022 \
>  --table TFL_central_lines\
>  --m 1 \
>  --target-dir /tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/log4j-slf4j-impl-2.13.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
25/11/27 20:32:46 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7.7.1.7.0-551
25/11/27 20:32:46 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
25/11/27 20:32:46 INFO manager.SqlManager: Using default fetchSize of 1000
25/11/27 20:32:46 INFO tool.CodeGenTool: Beginning code generation
25/11/27 20:32:47 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM "TFL_central_lines" AS t LIMIT 1
25/11/27 20:32:47 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce
warning: No SupportedSourceVersion annotation found on org.apache.hadoop.hdds.conf.ConfigFileGenerator, returning RELEASE_6.
warning: Supported source version 'RELEASE_6' from annotation processor 'org.apache.hadoop.hdds.conf.ConfigFileGenerator' less than -source '1.8'
2 warnings
25/11/27 20:32:49 WARN orm.CompilationManager: Could not rename /tmp/sqoop-Consultants/compile/0b3f6379b7595911daab7ef9ac12a64c/TFL_central_lines.java to /home/Consultants/./TFL_central_lines.java. Error: Destination '/home/Consultants/./TFL_central_lines.java' already exists
25/11/27 20:32:49 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-Consultants/compile/0b3f6379b7595911daab7ef9ac12a64c/TFL_central_lines.jar
25/11/27 20:32:49 WARN manager.PostgresqlManager: It looks like you are importing from postgresql.
25/11/27 20:32:49 WARN manager.PostgresqlManager: This transfer can be faster! Use the --direct
25/11/27 20:32:49 WARN manager.PostgresqlManager: option to exercise a postgresql-specific fast path.
25/11/27 20:32:49 INFO mapreduce.ImportJobBase: Beginning import of TFL_central_lines
25/11/27 20:32:49 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
25/11/27 20:32:49 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
25/11/27 20:32:49 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-3-80.eu-west-2.compute.internal/172.31.3.80:8032   
25/11/27 20:32:50 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/Consultants/.staging/job_1764063460733_0054
25/11/27 20:32:56 INFO db.DBInputFormat: Using read commited transaction isolation
25/11/27 20:32:56 INFO mapreduce.JobSubmitter: number of splits:1
25/11/27 20:32:56 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1764063460733_0054
25/11/27 20:32:56 INFO mapreduce.JobSubmitter: Executing with tokens: []
25/11/27 20:32:56 INFO conf.Configuration: resource-types.xml not found
25/11/27 20:32:56 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
25/11/27 20:32:57 INFO impl.YarnClientImpl: Submitted application application_1764063460733_0054
25/11/27 20:32:57 INFO mapreduce.Job: The url to track the job: http://ip-172-31-3-80.eu-west-2.compute.internal:8088/proxy/application_1764063460733_0054/
25/11/27 20:32:57 INFO mapreduce.Job: Running job: job_1764063460733_0054
25/11/27 20:33:26 INFO mapreduce.Job: Job job_1764063460733_0054 running in uber mode : false
25/11/27 20:33:26 INFO mapreduce.Job:  map 0% reduce 0%
25/11/27 20:33:33 INFO mapreduce.Job:  map 100% reduce 0%
25/11/27 20:33:34 INFO mapreduce.Job: Job job_1764063460733_0054 completed successfully
25/11/27 20:33:34 INFO mapreduce.Job: Counters: 33
        File System Counters
                FILE: Number of bytes read=0
                FILE: Number of bytes written=260329
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=85
                HDFS: Number of bytes written=866289
                HDFS: Number of read operations=6
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=1
                Other local map tasks=1
                Total time spent by all maps in occupied slots (ms)=3532
                Total time spent by all reduces in occupied slots (ms)=0
                Total time spent by all map tasks (ms)=3532
                Total vcore-milliseconds taken by all map tasks=14128
                Total megabyte-milliseconds taken by all map tasks=28934144
        Map-Reduce Framework
                Map input records=1547
                Map output records=1547
                Input split bytes=85
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=54
                CPU time spent (ms)=1780
                Physical memory (bytes) snapshot=417255424
                Virtual memory (bytes) snapshot=6647754752
                Total committed heap usage (bytes)=478150656
                Peak Map Physical memory (bytes)=417255424
                Peak Map Virtual memory (bytes)=6647754752
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=866289
25/11/27 20:33:34 INFO mapreduce.ImportJobBase: Transferred 845.9854 KB in 44.9441 seconds (18.823 KB/sec)
25/11/27 20:33:34 INFO mapreduce.ImportJobBase: Retrieved 1547 records.
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw
Found 3 items
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:14 /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:26 /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:33 /tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines
Found 2 items
-rw-r--r--   3 Consultants hadoop          0 2025-11-27 20:33 /tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines/_SUCCESS 
-rw-r--r--   3 Consultants hadoop     866289 2025-11-27 20:33 /tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines/part-m-00000
[Consultants@ip-172-31-8-235 ~]$ sqoop import \
>  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
>  --username Consultants \
>  --password WelcomeItc@2022 \
>  --table TFL_central_lines\
>  --m 1 \
>  --target-dir /tmp/DE011025/TFL_Batch_processing/raw/TFL_northern_lines
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/log4j-slf4j-impl-2.13.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
25/11/27 20:35:33 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7.7.1.7.0-551
25/11/27 20:35:33 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
25/11/27 20:35:33 INFO manager.SqlManager: Using default fetchSize of 1000
25/11/27 20:35:33 INFO tool.CodeGenTool: Beginning code generation
25/11/27 20:35:34 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM "TFL_central_lines" AS t LIMIT 1
25/11/27 20:35:34 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce
warning: No SupportedSourceVersion annotation found on org.apache.hadoop.hdds.conf.ConfigFileGenerator, returning RELEASE_6.
warning: Supported source version 'RELEASE_6' from annotation processor 'org.apache.hadoop.hdds.conf.ConfigFileGenerator' less than -source '1.8'
2 warnings
25/11/27 20:35:35 WARN orm.CompilationManager: Could not rename /tmp/sqoop-Consultants/compile/3cfdd47dea5b58118ec7032ee7124c1a/TFL_central_lines.java to /home/Consultants/./TFL_central_lines.java. Error: Destination '/home/Consultants/./TFL_central_lines.java' already exists
25/11/27 20:35:35 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-Consultants/compile/3cfdd47dea5b58118ec7032ee7124c1a/TFL_central_lines.jar
25/11/27 20:35:35 WARN manager.PostgresqlManager: It looks like you are importing from postgresql.
25/11/27 20:35:35 WARN manager.PostgresqlManager: This transfer can be faster! Use the --direct
25/11/27 20:35:35 WARN manager.PostgresqlManager: option to exercise a postgresql-specific fast path.
25/11/27 20:35:35 INFO mapreduce.ImportJobBase: Beginning import of TFL_central_lines
25/11/27 20:35:36 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
25/11/27 20:35:36 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
25/11/27 20:35:36 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-3-80.eu-west-2.compute.internal/172.31.3.80:8032
25/11/27 20:35:37 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/Consultants/.staging/job_1764063460733_0055
25/11/27 20:35:42 INFO db.DBInputFormat: Using read commited transaction isolation
25/11/27 20:35:43 INFO mapreduce.JobSubmitter: number of splits:1
25/11/27 20:35:43 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1764063460733_0055
25/11/27 20:35:43 INFO mapreduce.JobSubmitter: Executing with tokens: []
25/11/27 20:35:43 INFO conf.Configuration: resource-types.xml not found
25/11/27 20:35:43 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
25/11/27 20:35:43 INFO impl.YarnClientImpl: Submitted application application_1764063460733_0055
25/11/27 20:35:43 INFO mapreduce.Job: The url to track the job: http://ip-172-31-3-80.eu-west-2.compute.internal:8088/proxy/application_1764063460733_0055/
25/11/27 20:35:43 INFO mapreduce.Job: Running job: job_1764063460733_0055
25/11/27 20:35:49 INFO mapreduce.Job: Job job_1764063460733_0055 running in uber mode : false
25/11/27 20:35:49 INFO mapreduce.Job:  map 0% reduce 0%
25/11/27 20:36:10 INFO mapreduce.Job:  map 100% reduce 0%
25/11/27 20:36:10 INFO mapreduce.Job: Job job_1764063460733_0055 completed successfully
25/11/27 20:36:10 INFO mapreduce.Job: Counters: 33
        File System Counters
                FILE: Number of bytes read=0
                FILE: Number of bytes written=260325
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=85
                HDFS: Number of bytes written=866289
                HDFS: Number of read operations=6
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=1
                Other local map tasks=1
                Total time spent by all maps in occupied slots (ms)=18394
                Total time spent by all reduces in occupied slots (ms)=0
                Total time spent by all map tasks (ms)=18394
                Total vcore-milliseconds taken by all map tasks=73576
                Total megabyte-milliseconds taken by all map tasks=150683648
        Map-Reduce Framework
                Map input records=1547
                Map output records=1547
                Input split bytes=85
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=489
                CPU time spent (ms)=5290
                Physical memory (bytes) snapshot=298606592
                Virtual memory (bytes) snapshot=6620639232
                Total committed heap usage (bytes)=292028416
                Peak Map Physical memory (bytes)=298606592
                Peak Map Virtual memory (bytes)=6620639232
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=866289
25/11/27 20:36:10 INFO mapreduce.ImportJobBase: Transferred 845.9854 KB in 34.3355 seconds (24.6388 KB/sec)
25/11/27 20:36:10 INFO mapreduce.ImportJobBase: Retrieved 1547 records.
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw
Found 4 items
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:14 /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:26 /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:33 /tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:36 /tmp/DE011025/TFL_Batch_processing/raw/TFL_northern_lines
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw/TFL_northern_lines
Found 2 items
-rw-r--r--   3 Consultants hadoop          0 2025-11-27 20:36 /tmp/DE011025/TFL_Batch_processing/raw/TFL_northern_lines/_SUCCESS     
-rw-r--r--   3 Consultants hadoop     866289 2025-11-27 20:36 /tmp/DE011025/TFL_Batch_processing/raw/TFL_northern_lines/part-m-00000 
[Consultants@ip-172-31-8-235 ~]$ client_loop: send disconnect: Connection reset
PS C:\Users\Kavya\OneDrive\Desktop\TFL_Project> sqoop import \
>>  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
>>  --username Consultants \
>>  --password WelcomeItc@2022 \
>>  --table TFL_central_lines\
>>  --m 1 \
>>  --target-dir /tmp/DE011025/TFL_Batch_processing/raw/TFL_piccadilly_lines
>>
At line:2 char:4
+  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
+    ~
Missing expression after unary operator '--'.
At line:2 char:4
+  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
+    ~~~~~~~
Unexpected token 'connect' in expression or statement.
At line:3 char:4
+  --username Consultants \
+    ~
Missing expression after unary operator '--'.
At line:3 char:4
+  --username Consultants \
+    ~~~~~~~~
Unexpected token 'username' in expression or statement.
At line:4 char:4
+  --password WelcomeItc@2022 \
+    ~
Missing expression after unary operator '--'.
At line:4 char:4
+  --password WelcomeItc@2022 \
+    ~~~~~~~~
Unexpected token 'password' in expression or statement.
At line:5 char:4
+  --table TFL_central_lines\
+    ~
Missing expression after unary operator '--'.
At line:5 char:4
+  --table TFL_central_lines\
+    ~~~~~
Unexpected token 'table' in expression or statement.
At line:6 char:4
+  --m 1 \
+    ~
Missing expression after unary operator '--'.
At line:6 char:4
+  --m 1 \
+    ~
Unexpected token 'm' in expression or statement.
Not all parse errors were reported.  Correct the reported errors and try again.
    + CategoryInfo          : ParserError: (:) [], ParentContainsErrorRecordException
    + FullyQualifiedErrorId : MissingExpressionAfterOperator

PS C:\Users\Kavya\OneDrive\Desktop\TFL_Project> ssh Consultants@18.134.163.221
Consultants@18.134.163.221's password: 
Register this system with Red Hat Insights: insights-client --register
Create an account or view all your systems at https://red.ht/insights-dashboard
Last login: Thu Nov 27 20:56:59 2025 from 107.204.110.80
[Consultants@ip-172-31-8-235 ~]$ sqoop import \
>  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
>  --username Consultants \
>  --password WelcomeItc@2022 \
>  --table TFL_central_lines\
>  --m 1 \
>  --target-dir /tmp/DE011025/TFL_Batch_processing/raw/TFL_piccadilly_lines
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/log4j-slf4j-impl-2.13.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
25/11/27 21:06:15 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7.7.1.7.0-551
25/11/27 21:06:15 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
25/11/27 21:06:15 INFO manager.SqlManager: Using default fetchSize of 1000
25/11/27 21:06:15 INFO tool.CodeGenTool: Beginning code generation
25/11/27 21:06:16 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM "TFL_central_lines" AS t LIMIT 1
25/11/27 21:06:16 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce
warning: No SupportedSourceVersion annotation found on org.apache.hadoop.hdds.conf.ConfigFileGenerator, returning RELEASE_6.
warning: Supported source version 'RELEASE_6' from annotation processor 'org.apache.hadoop.hdds.conf.ConfigFileGenerator' less than -source '1.8'
2 warnings
25/11/27 21:06:17 WARN orm.CompilationManager: Could not rename /tmp/sqoop-Consultants/compile/3afd2b0204bdef7ce4e456358ea11a35/TFL_central_lines.java to /home/Consultants/./TFL_central_lines.java. Error: Destination '/home/Consultants/./TFL_central_lines.java' already exists
25/11/27 21:06:17 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-Consultants/compile/3afd2b0204bdef7ce4e456358ea11a35/TFL_central_lines.jar
25/11/27 21:06:17 WARN manager.PostgresqlManager: It looks like you are importing from postgresql.
25/11/27 21:06:17 WARN manager.PostgresqlManager: This transfer can be faster! Use the --direct
25/11/27 21:06:17 WARN manager.PostgresqlManager: option to exercise a postgresql-specific fast path.
25/11/27 21:06:17 INFO mapreduce.ImportJobBase: Beginning import of TFL_central_lines
25/11/27 21:06:18 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
25/11/27 21:06:18 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
25/11/27 21:06:18 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-3-80.eu-west-2.compute.internal/172.31.3.80:8032
25/11/27 21:06:19 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/Consultants/.staging/job_1764063460733_0056
25/11/27 21:06:26 INFO db.DBInputFormat: Using read commited transaction isolation
25/11/27 21:06:27 INFO mapreduce.JobSubmitter: number of splits:1
25/11/27 21:06:27 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1764063460733_0056
25/11/27 21:06:27 INFO mapreduce.JobSubmitter: Executing with tokens: []
25/11/27 21:06:27 INFO conf.Configuration: resource-types.xml not found
25/11/27 21:06:27 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
25/11/27 21:06:27 INFO impl.YarnClientImpl: Submitted application application_1764063460733_0056
25/11/27 21:06:27 INFO mapreduce.Job: The url to track the job: http://ip-172-31-3-80.eu-west-2.compute.internal:8088/proxy/application_1764063460733_0056/
25/11/27 21:06:27 INFO mapreduce.Job: Running job: job_1764063460733_0056
25/11/27 21:06:33 INFO mapreduce.Job: Job job_1764063460733_0056 running in uber mode : false
25/11/27 21:06:33 INFO mapreduce.Job:  map 0% reduce 0%
25/11/27 21:06:56 INFO mapreduce.Job:  map 100% reduce 0%
25/11/27 21:06:57 INFO mapreduce.Job: Job job_1764063460733_0056 completed successfully
25/11/27 21:06:58 INFO mapreduce.Job: Counters: 33
        File System Counters
                FILE: Number of bytes read=0
                FILE: Number of bytes written=260327
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=85
                HDFS: Number of bytes written=866289
                HDFS: Number of read operations=6
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=1
                Other local map tasks=1
                Total time spent by all maps in occupied slots (ms)=20960
                Total time spent by all reduces in occupied slots (ms)=0
                Total time spent by all map tasks (ms)=20960
                Total vcore-milliseconds taken by all map tasks=83840
                Total megabyte-milliseconds taken by all map tasks=171704320
        Map-Reduce Framework
                Map input records=1547
                Map output records=1547
                Input split bytes=85
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=710
                CPU time spent (ms)=7070
                Physical memory (bytes) snapshot=324620288
                Virtual memory (bytes) snapshot=6635868160
                Total committed heap usage (bytes)=303562752
                Peak Map Physical memory (bytes)=324620288
                Peak Map Virtual memory (bytes)=6635868160
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=866289
25/11/27 21:06:58 INFO mapreduce.ImportJobBase: Transferred 845.9854 KB in 39.3965 seconds (21.4736 KB/sec)
25/11/27 21:06:58 INFO mapreduce.ImportJobBase: Retrieved 1547 records.
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw
Found 5 items
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:14 /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:26 /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:33 /tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:36 /tmp/DE011025/TFL_Batch_processing/raw/TFL_northern_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 21:06 /tmp/DE011025/TFL_Batch_processing/raw/TFL_piccadilly_lines
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw/TFL_piccadilly_lines
Found 2 items
-rw-r--r--   3 Consultants hadoop          0 2025-11-27 21:06 /tmp/DE011025/TFL_Batch_processing/raw/TFL_piccadilly_lines/_SUCCESS   
-rw-r--r--   3 Consultants hadoop     866289 2025-11-27 21:06 /tmp/DE011025/TFL_Batch_processing/raw/TFL_piccadilly_lines/part-m-00000
[Consultants@ip-172-31-8-235 ~]$ sqoop import \
>  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
>  --username Consultants \
>  --password WelcomeItc@2022 \
>  --table TFL_central_lines\
>  --m 1 \
>  --target-dir /tmp/DE011025/TFL_Batch_processing/raw/TFL_victoria_lines
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/log4j-slf4j-impl-2.13.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
25/11/27 21:09:48 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7.7.1.7.0-551
25/11/27 21:09:48 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
25/11/27 21:09:49 INFO manager.SqlManager: Using default fetchSize of 1000
25/11/27 21:09:49 INFO tool.CodeGenTool: Beginning code generation
25/11/27 21:09:49 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM "TFL_central_lines" AS t LIMIT 1
25/11/27 21:09:49 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce
warning: No SupportedSourceVersion annotation found on org.apache.hadoop.hdds.conf.ConfigFileGenerator, returning RELEASE_6.
warning: Supported source version 'RELEASE_6' from annotation processor 'org.apache.hadoop.hdds.conf.ConfigFileGenerator' less than -source '1.8'
2 warnings
25/11/27 21:09:51 WARN orm.CompilationManager: Could not rename /tmp/sqoop-Consultants/compile/f0d1e2a05a64d62cd3941226183d8c2b/TFL_central_lines.java to /home/Consultants/./TFL_central_lines.java. Error: Destination '/home/Consultants/./TFL_central_lines.java' already exists
25/11/27 21:09:51 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-Consultants/compile/f0d1e2a05a64d62cd3941226183d8c2b/TFL_central_lines.jar
25/11/27 21:09:51 WARN manager.PostgresqlManager: It looks like you are importing from postgresql.
25/11/27 21:09:51 WARN manager.PostgresqlManager: This transfer can be faster! Use the --direct
25/11/27 21:09:51 WARN manager.PostgresqlManager: option to exercise a postgresql-specific fast path.
25/11/27 21:09:51 INFO mapreduce.ImportJobBase: Beginning import of TFL_central_lines
25/11/27 21:09:51 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
25/11/27 21:09:51 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
25/11/27 21:09:52 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-3-80.eu-west-2.compute.internal/172.31.3.80:8032   
25/11/27 21:09:52 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/Consultants/.staging/job_1764063460733_0057
25/11/27 21:09:59 INFO db.DBInputFormat: Using read commited transaction isolation
25/11/27 21:09:59 INFO mapreduce.JobSubmitter: number of splits:1
25/11/27 21:09:59 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1764063460733_0057
25/11/27 21:09:59 INFO mapreduce.JobSubmitter: Executing with tokens: []
25/11/27 21:09:59 INFO conf.Configuration: resource-types.xml not found
25/11/27 21:09:59 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
25/11/27 21:10:00 INFO impl.YarnClientImpl: Submitted application application_1764063460733_0057
25/11/27 21:10:00 INFO mapreduce.Job: The url to track the job: http://ip-172-31-3-80.eu-west-2.compute.internal:8088/proxy/application_1764063460733_0057/
25/11/27 21:10:00 INFO mapreduce.Job: Running job: job_1764063460733_0057
25/11/27 21:10:27 INFO mapreduce.Job: Job job_1764063460733_0057 running in uber mode : false
25/11/27 21:10:27 INFO mapreduce.Job:  map 0% reduce 0%
25/11/27 21:10:43 INFO mapreduce.Job: Task Id : attempt_1764063460733_0057_m_000000_0, Status : FAILED
Error: java.lang.RuntimeException: org.postgresql.util.PSQLException: The connection attempt failed.
        at org.apache.sqoop.mapreduce.db.DBInputFormat.getConnection(DBInputFormat.java:226)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.setDbConf(DBInputFormat.java:170)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.setConf(DBInputFormat.java:160)
        at org.apache.hadoop.util.ReflectionUtils.setConf(ReflectionUtils.java:77)
        at org.apache.hadoop.util.ReflectionUtils.newInstance(ReflectionUtils.java:137)
        at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:763)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:347)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1898)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)
Caused by: org.postgresql.util.PSQLException: The connection attempt failed.
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:297)
        at org.postgresql.core.ConnectionFactory.openConnection(ConnectionFactory.java:49)
        at org.postgresql.jdbc.PgConnection.<init>(PgConnection.java:217)
        at org.postgresql.Driver.makeConnection(Driver.java:458)
        at org.postgresql.Driver.connect(Driver.java:260)
        at java.sql.DriverManager.getConnection(DriverManager.java:664)
        at java.sql.DriverManager.getConnection(DriverManager.java:247)
        at org.apache.sqoop.mapreduce.db.DBConfiguration.getConnection(DBConfiguration.java:300)
        at org.apache.sqoop.mapreduce.db.DBInputFormat.getConnection(DBInputFormat.java:218)
        ... 11 more
Caused by: java.net.SocketTimeoutException: connect timed out
        at java.net.PlainSocketImpl.socketConnect(Native Method)
        at java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:350)
        at java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:206)
        at java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:188)
        at java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392)
        at java.net.Socket.connect(Socket.java:607)
        at org.postgresql.core.PGStream.<init>(PGStream.java:81)
        at org.postgresql.core.v3.ConnectionFactoryImpl.tryConnect(ConnectionFactoryImpl.java:93)
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:197)
        ... 19 more

25/11/27 21:10:51 INFO mapreduce.Job:  map 100% reduce 0%
25/11/27 21:10:52 INFO mapreduce.Job: Job job_1764063460733_0057 completed successfully
25/11/27 21:10:52 INFO mapreduce.Job: Counters: 34
        File System Counters
                FILE: Number of bytes read=0
                FILE: Number of bytes written=260325
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=85
                HDFS: Number of bytes written=866289
                HDFS: Number of read operations=6
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Failed map tasks=1
                Launched map tasks=2
                Other local map tasks=2
                Total time spent by all maps in occupied slots (ms)=18415
                Total time spent by all reduces in occupied slots (ms)=0
                Total time spent by all map tasks (ms)=18415
                Total vcore-milliseconds taken by all map tasks=73660
                Total megabyte-milliseconds taken by all map tasks=150855680
        Map-Reduce Framework
                Map input records=1547
                Map output records=1547
                Input split bytes=85
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=49
                CPU time spent (ms)=1950
                Physical memory (bytes) snapshot=332263424
                Virtual memory (bytes) snapshot=6639181824
                Total committed heap usage (bytes)=352845824
                Peak Map Physical memory (bytes)=335298560
                Peak Map Virtual memory (bytes)=6639181824
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=866289
25/11/27 21:10:52 INFO mapreduce.ImportJobBase: Transferred 845.9854 KB in 60.6242 seconds (13.9546 KB/sec)
25/11/27 21:10:52 INFO mapreduce.ImportJobBase: Retrieved 1547 records.
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw
Found 6 items
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:14 /tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:26 /tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:33 /tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 20:36 /tmp/DE011025/TFL_Batch_processing/raw/TFL_northern_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 21:06 /tmp/DE011025/TFL_Batch_processing/raw/TFL_piccadilly_lines
drwxr-xr-x   - Consultants hadoop          0 2025-11-27 21:10 /tmp/DE011025/TFL_Batch_processing/raw/TFL_victoria_lines
[Consultants@ip-172-31-8-235 ~]$ hdfs dfs -ls /tmp/DE011025/TFL_Batch_processing/raw/TFL_victoria_lines
Found 2 items
-rw-r--r--   3 Consultants hadoop          0 2025-11-27 21:10 /tmp/DE011025/TFL_Batch_processing/raw/TFL_victoria_lines/_SUCCESS     
-rw-r--r--   3 Consultants hadoop     866289 2025-11-27 21:10 /tmp/DE011025/TFL_Batch_processing/raw/TFL_victoria_lines/part-m-00000 
[Consultants@ip-172-31-8-235 ~]$ 