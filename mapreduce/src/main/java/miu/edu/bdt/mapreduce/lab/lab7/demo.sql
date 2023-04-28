sqoop import
    --connect jdbc:mysql://quickstart.cloudera/cs523
    --username root -P
    --table stocks
    --target-dir=/user/cloudera/sqoopImportOutput
    -m 4
    --columns "id, symbol, open_price"
    --where "low_price < 100"
    --direct
    --fields-terminated-by "\t"
    --append

sqoop export
    --connect jdbc:mysql://quickstart.cloudera/cs523
    --username root -P
    --table student
    --export-dir=/user/cloudera/sqoopImportOutput/
    -m 4
    --update-key id
    --update-mode updateonly

create table stocks
(
    id         int not null primary key,
    symbol     varchar(10),
    quote_date datetime,
    open_price float,
    high_price float,
    low_price  float
);

insert into stocks
values (1, 'AAPL', '2009-01-02', 85.88, 91.04, 85.16),
       (2, 'AAPL', '2008-01-02', 199.27, 200.26, 192.55),
       (3, 'AAPL', '2008-01-03', 86.29, 86.55, 81.19);


myAgent.sources = mySource
myAgent.channels = fileChannel memoryChannel
myAgent.sinks = hdfsSink loggerSink

myAgent.sources.mySource.type = spooldir
myAgent.sources.mySource.spoolDir = /home/cloudera/cs523/flume/log
myAgent.sources.mySource.channels = fileChannel memoryChannel
myAgent.sources.mySource.fileHeader = true
myAgent.sources.mySource.interceptors = ts
myAgent.sources.mySource.interceptors.ts.type = timestamp

myAgent.channels.memoryChannel.type = memory
myAgent.channels.memoryChannel.capacity = 200

myAgent.channels.fileChannel.type = file

myAgent.sinks.hdfsSink.type = hdfs
myAgent.sinks.hdfsSink.hdfs.path = hdfs://quickstart.cloudera/user/cloudera/flumeImport/
myAgent.sinks.hdfsSink.hdfs.filePrefix = myFlume
myAgent.sinks.hdfsSink.hdfs.fileType = DataStream
myAgent.sinks.hdfsSink.hdfs.rollInterval = 3000
myAgent.sinks.hdfsSink.hdfs.rollSize = 3000
myAgent.sinks.hdfsSink.hdfs.rollCount = 0
myAgent.sinks.hdfsSink.channel = fileChannel

myAgent.sinks.loggerSink.type = logger
myAgent.sinks.loggerSink.channel = memoryChannel

flume-ng agent -n myAgent -c cs523/flume/conf/ -f cs523/flume/conf/myFlumeConf.conf