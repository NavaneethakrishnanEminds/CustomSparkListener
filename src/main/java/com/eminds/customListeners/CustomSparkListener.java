package com.eminds.customListeners;

import org.apache.spark.executor.*;
import org.apache.spark.scheduler.*;
import org.apache.spark.util.AccumulatorV2;
import scala.collection.Iterator;

import java.util.Properties;

public class CustomSparkListener extends SparkListener {

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        String appName = applicationStart.appName();
        String appId = applicationStart.appId().get();
        long startTime = applicationStart.time();
        String sparkUser = applicationStart.sparkUser();
        String appAttemptId = applicationStart.appAttemptId().get();

        printLine();
        System.out.println("Application Started:");
        System.out.println("Name: " + appName);
        System.out.println("ID: " + appId);
        System.out.println("Start Time: " + startTime);
        System.out.println("User: " + sparkUser);
        System.out.println("Attempt ID: " + appAttemptId);
        printLine();
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        long endTime = applicationEnd.time();
        printLine();
        System.out.println("Application Ended:");
        System.out.println("End Time: " + endTime);
        printLine();
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        int jobId = jobStart.jobId();
        long startTime = jobStart.time();
        Properties properties = jobStart.properties();

        printLine();
        System.out.println("Job Started:");
        System.out.println("Job ID: " + jobId);
        System.out.println("Start Time: " + startTime);
        System.out.println("Properties: " + properties);
        printLine();
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        int jobId = jobEnd.jobId();
        long endTime = jobEnd.time();
        JobResult jobResult = jobEnd.jobResult();

        printLine();
        System.out.println("Job Ended:");
        System.out.println("Job ID: " + jobId);
        System.out.println("End Time: " + endTime);
        System.out.println("Result: " + jobResult);
        printLine();
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        StageInfo stageInfo = stageSubmitted.stageInfo();
        int stageId = stageInfo.stageId();
        String stageName = stageInfo.name();
        int numTasks = stageInfo.numTasks();
        Properties properties = stageSubmitted.properties();

        printLine();
        System.out.println("Stage Submitted:");
        System.out.println("Stage ID: " + stageId);
        System.out.println("Stage Name: " + stageName);
        System.out.println("Number of Tasks: " + numTasks);
        System.out.println("Properties: " + properties);
        printLine();
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {

        printLine();
        System.out.println("Stage Completed:");
        System.out.println("Stage ID = " + stageCompleted.stageInfo().stageId());
        System.out.println("Stage Name = " + stageCompleted.stageInfo().name());
        System.out.println("Stage details = " + stageCompleted.stageInfo().details());
        System.out.println("Stage Task count = " + stageCompleted.stageInfo().numTasks());
        System.out.println("Stage Status = " + stageCompleted.stageInfo().getStatusString());
        printLine();
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        TaskInfo taskInfo = taskStart.taskInfo();
        long taskId = taskInfo.taskId();
        int index = taskInfo.index();
        long launchTime = taskInfo.launchTime();
        String executorId = taskInfo.executorId();
        String host = taskInfo.host();

        printLine();
        System.out.println("Task Started:");
        System.out.println("Task ID: " + taskId);
        System.out.println("Index: " + index);
        System.out.println("Launch Time: " + launchTime);
        System.out.println("Executor ID: " + executorId);
        System.out.println("Host: " + host);
        printLine();
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        TaskInfo taskInfo = taskEnd.taskInfo();
        long taskId = taskInfo.taskId();
        int index = taskInfo.index();
        long duration = taskInfo.duration();
        String executorId = taskInfo.executorId();
        String host = taskInfo.host();

        TaskMetrics taskMetrics = taskEnd.taskMetrics();
        long executorDeserializeTime = taskMetrics.executorDeserializeTime();
        long executorRunTime = taskMetrics.executorRunTime();
        long resultSize = taskMetrics.resultSize();
        long jvmGCTime = taskMetrics.jvmGCTime();
        long memoryBytesSpilled = taskMetrics.memoryBytesSpilled();
        long diskBytesSpilled = taskMetrics.diskBytesSpilled();

        InputMetrics inputMetrics = taskMetrics.inputMetrics();
        OutputMetrics outputMetrics = taskMetrics.outputMetrics();
        ShuffleReadMetrics shuffleReadMetrics = taskMetrics.shuffleReadMetrics();
        ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();

        printLine();
        System.out.println("Task Ended:");
        System.out.println("Task ID: " + taskId);
        System.out.println("Index: " + index);
        System.out.println("Duration: " + duration);
        System.out.println("Executor ID: " + executorId);
        System.out.println("Host: " + host);
        System.out.println("Executor Deserialize Time: " + executorDeserializeTime);
        System.out.println("Executor Run Time: " + executorRunTime);
        System.out.println("Result Size: " + resultSize);
        System.out.println("JVM GC Time: " + jvmGCTime);
        System.out.println("Memory Bytes Spilled: " + memoryBytesSpilled);
        System.out.println("Disk Bytes Spilled: " + diskBytesSpilled);
        printLine();

        if (inputMetrics != null) {
            printStarLine();
            System.out.println("Input Metrics:");
            System.out.println("Bytes Read: " + inputMetrics.bytesRead());
            System.out.println("Records Read: " + inputMetrics.recordsRead());
            printLine();
        }

        if (outputMetrics != null) {
            printStarLine();
            System.out.println("Output Metrics:");
            System.out.println("Bytes Written: " + outputMetrics.bytesWritten());
            System.out.println("Records Written: " + outputMetrics.recordsWritten());
            printLine();
        }

        if (shuffleReadMetrics != null) {
            printStarLine();
            System.out.println("Shuffle Read Metrics:");
            System.out.println("Remote Blocks Fetched: " + shuffleReadMetrics.remoteBlocksFetched());
            printLine();
        }
    }

    public void printLine(){
        System.out.println("***********************************************************");
        System.out.println();
    }

    public void printStarLine(){
        System.out.println("***********************************************************");
    }
}


