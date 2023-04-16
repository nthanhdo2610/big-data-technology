sqoop import --connect jdbc:mysql://quickstart.cloudera/cs523 --username root -P --table stocks --columns "id, symbol, open_price" --where "low_price < 100" --direct --fields-terminated-by "\t" --target-dir=/user/cloudera/sqoopImportOutput

sqoop export --export-dir=/user/cloudera/sqoopImportOutput/ --connect jdbc:mysql://quickstart.cloudera/cs523 --username root -P --table student -m 1

flume-ng agent -n agent1 -c cs523/flume/conf/ -f cs523/flume/conf/myFlumeConf.conf


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


agent1.sources = mySource
agent1.channels = ch1
agent1.sinks = hdfsSink

agent1.sources.mySource.type = exec
agent1.sources.mySource.channels = ch1
agent1.sources.mySource.command = /home/cloudera/cs523/flume/log/

agent1.sinks.hdfsSink.type = hdfs
agent1.sinks.hdfsSink.hdfs.path = hdfs://quickstart.cloudera/user/cloudera/flumeImport/
agent1.sinks.hdfsSink.hdfs.filePrefix = myFlume
agent1.sinks.hdfsSink.hdfs.fileType = DataStream
agent1.sinks.hdfsSink.hdfs.rollInterval = 3000
agent1.sinks.hdfsSink.hdfs.rollSize = 300
agent1.sinks.hdfsSink.hdfs.rollCount = 0
agent1.sinks.hdfsSink.channel = ch1

agent1.channels.ch1.type = memory
agent1.channels.ch1.capacity = 200