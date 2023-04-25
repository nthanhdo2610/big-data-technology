# big-data-technology


Course: Big Data Technology (CS-523)

Professor: Mrudula Mukadam


Group Members

	1.	Thanh Do Nguyen
	2.	Van Ngoc Trau Nguyen
	3.	Mihret Ghebremichael Kidane
	4.	Melaku Gossaye Denbel
	

# Download Kafka:
 	- Link: https://archive.apache.org/dist/kafka/2.4.1/kafka_2.12-2.4.1.tgz
 	
 	- Extract the downloaded file, then go to this directory:
 	
     	+ cd /home/cloudera/Downloads/kafka_2.12-2.4.1

# Check service status:
     + service --status-all
 
# Start Zookeeper if it's not running (usually it already started):
     + bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server:
     + bin/kafka-server-start.sh -daemon config/server.properties

# Build maven project:
     + cd ../project
     + mvn clean package
     
# Run jar file: always run jar file with dependencies
     + java -jar 'consumer/target/consumer-1.0-SNAPSHOT-jar-with-dependencies.jar'
     + java -jar 'producer/target/producer-1.0-SNAPSHOT-jar-with-dependencies.jar'
     
# Run visualizer with jar file: run jar file (without dependencies)
	 + java -jar 'visualizer/visualizer-1.0-SNAPSHOT.jar'
     
#Analyze projects:
     + Please refer the FinalProject_presentation_v2.pptx