package com.eminds;

import com.eminds.customListeners.CustomSparkListener;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.shaded.org.xbill.DNS.Options.set;

public class SparkApp {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("Custom Spark Listener Example")
                .setMaster("yarn")
                .set("spark.executor.memory", "2g")
                .set("spark.executor.cores", "2")
                .set("spark.driver.memory", "1g");
//                .set("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8088")
//                .set("spark.submit.deployMode", "cluster");
                //.setMaster("spark://resourcemanager:8088");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.sc().addSparkListener(new CustomSparkListener());

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        System.out.println("Number of partitions: " + rdd.getNumPartitions());

        JavaRDD<Integer> mappedRDD = rdd.map(x -> x * 2);

        JavaRDD<Integer> filteredRDD = mappedRDD.filter(x -> x > 10);

        JavaRDD<String> shuffledRDD = filteredRDD.map(x -> (x % 2 == 0 ? "Even" : "Odd") + ": " + x);

        List<String> result = shuffledRDD.collect();

        System.out.println("Filtered and Shuffled Data:");
        result.forEach(System.out::println);

    }
}
