package it.polito.bigdata.spark.example;
import it.polito.bigdata.spark.example.DateTool;
import scala.Tuple2;

import org.apache.spark.api.java.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: "+JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");

		// TODO
		// .....
		// .....
		// .....
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		JavaRDD<String> inputRDD2 = sc.textFile(inputPath2);
		
		JavaPairRDD<String, Double> x = inputRDD.filter(line -> !line.startsWith("station") || !line.endsWith("0\\t0")).mapToPair(
				line -> {
					String[] fields = line.split("[ \\t]+"); //split sia per gli spazi che per i tab
					String day = DateTool.DayOfTheWeek(fields[1]);
					String hour = fields[2].split(":")[0];
					String myKey = fields[0] + " " + day + " " + hour;
				    return new Tuple2<String,Tuple2<Integer,Integer>>(myKey, new Tuple2<Integer,Integer>(fields[4].equals("0") ? 1:0,1));
				}
		).reduceByKey((x1,x2)-> new Tuple2<Integer,Integer>(x1._1+x2._1,x1._2+x2._2))
		.filter(y -> y._2._1/y._2._2 >= threshold)
		.mapToPair(y-> new Tuple2<String,Double>(y._1, (double) y._2._1/y._2._2));
		

		// Store in resultKML one String, representing a KML marker, for each station 
		// with a critical timeslot 
		// JavaRDD<String> resultKML = .....
		  
		// Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
		// resultKML.coalesce(1).saveAsTextFile(outputFolder); 

		// Close the Spark context
		sc.close();
	}
}
