package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #6").setMaster("local");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		// Remember to remove .setMaster("local") before running your application on the
		// cluster

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: " + JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");

		// single Spark application that: 1. Transposes the original Amazon food
		// dataset, obtaining a PairRDD in which there is one pair, structured as
		// follows, for each user:(user_id, list of product_ids reviewed by user_id) The
		// returned PairRDD contains one pair for each user, which contains the user_id
		// and the complete list of (distinct) products reviewed by that user. If user
		// user_id reviewed more times the same product, that product must occur only
		// one time in the returned list of product_ids reviewed by user_id;

		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		JavaPairRDD<String, String> x = inputRDD.filter(line -> !line.startsWith("Id")).mapToPair(
				line -> {
					String[] words = line.split(",");
					return new Tuple2<String, String>(words[2], words[1]);
				}).distinct();
		/*
		 * ora ho tutte le coppie (user_id, product_id)
		 * A2 B1
		 * A5 B3
		 * A4 B4
		 * A5 B5
		 * A5 B1
		 * A1 B2
		 * A2 B3
		 * A3 B3
		 * A4 B5
		 * A4 B1
		 * A4 B3
		 * A2 B5
		 */

		// JavaPairRDD<String, String> y = x.reduceByKey((s1, s2) -> s1 + "," + s2);
		/*
		 * ora ho tutte le coppie (user_id,lista di product_id)
		 * A3 B3
		 * A4 B4,B5,B1,B3
		 * A5 B3,B5,B1
		 * A1 B2
		 * A2 B1,B3,B5
		 */
		JavaPairRDD<String, Iterable<String>> z = x.groupByKey();
		/*
		 * ora ho tutte le coppie (user_id,lista di product_id)
		 * A3 [B3]
		 * A4 [B4,B5,B1,B3]
		 * A5 [B3,B5,B1]
		 * A1 [B2]
		 * A2 [B1,B3,B5]
		 */

		JavaPairRDD<String, Integer> resultRDD = z.flatMapToPair(
				line -> {
					List<String> values = IteratorUtils.toList(line._2.iterator());
					List<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();

					// Ordina i valori in ordine alfabetico
					Collections.sort(values);

					for (int i = 0; i < values.size(); i++) {
						for (int j = i + 1; j < values.size(); j++) {

							Tuple2<String, Integer> tuple;
							if (values.get(i).compareTo(values.get(j)) < 0) {
								tuple = new Tuple2<String, Integer>(values.get(i) + "," + values.get(j), 1);
							} else {
								tuple = new Tuple2<String, Integer>(values.get(j) + "," + values.get(i), 1);
							}

							result.add(tuple);

						}
					}

					return result.iterator();
				}).reduceByKey((x1, x2) -> x1 + x2);

		/*
		 * (B3,B5,3)
		 * (B1,B3,3)
		 * (B3,B4,1)
		 * (B1,B5,3)
		 * (B1,B4,1)
		 * (B4,B5,1)
		 */


		// Ordina in ordine decrescente sulla base della frequenza
		JavaPairRDD<String, Integer> sortedPairCountRDD = resultRDD
				.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
				.sortByKey(false)
				.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));

		List<Tuple2<String, Integer>> bonusTask = resultRDD
				.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
				.sortByKey(false)
				.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
				.take(10);

		// Store the result in the output folder
		// sortedPairCountRDD.saveAsTextFile(outputPath);
		System.out.println("Top 10 coppie: " + bonusTask);

		// Store the result in the output folder
		sortedPairCountRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
