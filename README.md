# KafkaConsumerHDFSAppend
This code makes you go through, how to write a Kafka consumer program to append data to HDFS using Eclipse and Horton Work Sanbox

The File App.java Includes a java code to fetch data from Kafka Topic and calls a method of HDFSAppendTrial, to store the data in file saved on HDFS.

App.java file where we set properties of consumer, create a KafkaConsumer and set the required configurations, coreSite path, hdfsSite path, and path for the file where consumer data needs to be appended, which are helpful to create filesystem using other methods of other class called HDFSAppendTrial.Java.

pom.xml sets the depedencies required for a maven project and plugins that are to be use.

If Any suggestions of edits or to make my code more appropriate is appreciated ! 
