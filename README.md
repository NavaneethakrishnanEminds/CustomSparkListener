# Custom SparkListener Example

This project demonstrates how to create and use a Custom SparkListener in a Spark application to monitor various events during the execution of a Spark job, including application, job, stage, and task lifecycle events. The listener logs detailed information about the application's execution, helping in debugging, performance analysis, and tracking of resource utilization.

## Table of Contents
* [Overview](#overview)
* [Features](#features)
* [Setup](#setup)
* [Sample Output](#sample-output)

## Overview
This project demonstrates how to create and use a **Custom SparkListener** in a Spark application to monitor various events during the execution of a Spark job, including application, job, stage, and task lifecycle events. The listener logs detailed information about the application's execution, helping in debugging, performance analysis, and tracking of resource utilization.

* Application start and end
* Job start and end
* Stage submission and completion
* Task start and end

Each event is logged with detailed metrics, such as task execution times, stage properties, and shuffle read/write metrics. This provides deep insights into the application's execution and is particularly useful for debugging, performance monitoring, and resource utilization tracking.

## Features
* Application Monitoring: Logs when the Spark application starts and ends.
* Job Monitoring: Logs job-specific details like job ID, start time, and properties.
* Stage Monitoring: Logs information about stages, including stage ID, name, task count, and properties.
* Task Monitoring: Logs detailed task-level metrics such as executor ID, task duration, memory and disk spills, and shuffle metrics.
* Performance Metrics: Logs executor deserialize and runtime, JVM GC time, memory and disk usage, and shuffle operations.

## Setup
To set up and run the project locally or in a cluster (like YARN or Spark Standalone), follow these steps:

### Prerequisites
* Apache Spark 3.x or higher
* Java 8 or higher
* Maven (or another build tool) for dependency management and building the application
* A running Spark cluster or YARN environment (if testing in a distributed setup)

### Clone the repository:

````bash
https://github.com/NavaneethakrishnanEminds/CustomSparkListener.git
````
### Build the project using Maven:

````bash
mvn clean install
````
### Run the application:

````bash
/usr/local/spark/bin/spark-submit   --master yarn  --deploy-mode cluster  --class com.eminds.SparkApp /home/hadoop/data/javaCustomizedSparkListener-1.0-SNAPSHOT-jar-with-dependencies.jar
````

## Sample Output:

````
Number of partitions: 4
***********************************************************

Job Started:
Job ID: 0
Start Time: 1738045514313
Properties: {spark.rdd.scope={"id":"4","name":"collect"}, spark.rdd.scope.noOverride=true}
***********************************************************

***********************************************************

Stage Submitted:
Stage ID: 0
Stage Name: collect at SparkApp.java:42
Number of Tasks: 4
Properties: {spark.rdd.scope={"id":"4","name":"collect"}, resource.executor.cores=2, spark.rdd.scope.noOverride=true}
***********************************************************

***********************************************************

Task Started:
Task ID: 0
Index: 0
Launch Time: 1738045514434
Executor ID: 1
Host: nodemanager
***********************************************************

***********************************************************

Task Started:
Task ID: 1
Index: 1
Launch Time: 1738045514449
Executor ID: 2
Host: nodemanager
***********************************************************

***********************************************************

Task Started:
Task ID: 2
Index: 2
Launch Time: 1738045514450
Executor ID: 1
Host: nodemanager
***********************************************************

***********************************************************

Task Started:
Task ID: 3
Index: 3
Launch Time: 1738045514451
Executor ID: 2
Host: nodemanager
***********************************************************

***********************************************************

Task Ended:
Task ID: 1
Index: 1
Duration: 692
Executor ID: 2
Host: nodemanager
Executor Deserialize Time: 530
Executor Run Time: 73
Result Size: 981
JVM GC Time: 0
Memory Bytes Spilled: 0
Disk Bytes Spilled: 0
***********************************************************

***********************************************************
Input Metrics:
Bytes Read: 0
Records Read: 0
***********************************************************

***********************************************************
Output Metrics:
Bytes Written: 0
Records Written: 0
***********************************************************

***********************************************************
Shuffle Read Metrics:
Remote Blocks Fetched: 0
***********************************************************

***********************************************************

Task Ended:
Task ID: 3
Index: 3
Duration: 695
Executor ID: 2
Host: nodemanager
Executor Deserialize Time: 536
Executor Run Time: 71
Result Size: 1014
JVM GC Time: 0
Memory Bytes Spilled: 0
Disk Bytes Spilled: 0
***********************************************************

***********************************************************
Input Metrics:
Bytes Read: 0
Records Read: 0
***********************************************************

***********************************************************
Output Metrics:
Bytes Written: 0
Records Written: 0
***********************************************************

***********************************************************
Shuffle Read Metrics:
Remote Blocks Fetched: 0
***********************************************************

***********************************************************

Task Ended:
Task ID: 2
Index: 2
Duration: 752
Executor ID: 1
Host: nodemanager
Executor Deserialize Time: 617
Executor Run Time: 69
Result Size: 1003
JVM GC Time: 0
Memory Bytes Spilled: 0
Disk Bytes Spilled: 0
***********************************************************

***********************************************************
Input Metrics:
Bytes Read: 0
Records Read: 0
***********************************************************

***********************************************************
Output Metrics:
Bytes Written: 0
Records Written: 0
***********************************************************

***********************************************************
Shuffle Read Metrics:
Remote Blocks Fetched: 0
***********************************************************

***********************************************************

Task Ended:
Task ID: 0
Index: 0
Duration: 769
Executor ID: 1
Host: nodemanager
Executor Deserialize Time: 631
Executor Run Time: 55
Result Size: 981
JVM GC Time: 0
Memory Bytes Spilled: 0
Disk Bytes Spilled: 0
***********************************************************

***********************************************************
Input Metrics:
Bytes Read: 0
Records Read: 0
***********************************************************

***********************************************************
Output Metrics:
Bytes Written: 0
Records Written: 0
***********************************************************

***********************************************************
Shuffle Read Metrics:
Remote Blocks Fetched: 0
***********************************************************

***********************************************************

Stage Completed:
Stage ID = 0
Stage Name = collect at SparkApp.java:42
Stage details = org.apache.spark.api.java.AbstractJavaRDDLike.collect(JavaRDDLike.scala:45)
com.eminds.SparkApp.main(SparkApp.java:42)
java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
java.base/java.lang.reflect.Method.invoke(Method.java:566)
org.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:738)
Stage Task count = 4
Stage Status = succeeded
***********************************************************

***********************************************************

Job Ended:
Job ID: 0
End Time: 1738045515211
Result: JobSucceeded
***********************************************************

Filtered and Shuffled Data:
Even: 12
Even: 14
Even: 16
Even: 18
Even: 20
***********************************************************

Application Ended:
End Time: 1738045515236
***********************************************************
````
