package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		String prefix;
		
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: "+JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");
		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);
		
		/*
		 * Task 1
		*/
		JavaRDD<String> wordsFiltered = wordFreqRDD.filter(pair -> pair.startsWith(prefix));
		int count = wordsFiltered.collect().size();
		int max = wordsFiltered.map(pair -> Integer.parseInt(pair.split("\t")[1])).reduce((x,y) -> Math.max(x, y));
		
		System.out.println("Number of words starting with " + prefix + ": " + count);
		System.out.println("Max frequency: " + max);
		//wordsFiltered.saveAsTextFile(outputPath);
		
		/*
		 * Task 2
		 */
		double threshold = 0.8*max;
		System.out.println("Threshold: " + threshold);
		JavaRDD<String> wordsFiltered2 = wordsFiltered.filter(x -> Double.parseDouble(x.split("\t")[1]) >= threshold);
		int count2 = wordsFiltered2.collect().size();
		System.out.println("Number of words starting with '" + prefix + "' and greater than threshold ("+threshold+"): " + count2);

		JavaRDD<String> wordsFiltered3 = wordsFiltered2.map(x->x.split("\t")[0]);
		wordsFiltered3.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
