package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import java.util.Arrays;

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
		System.out.println("ApplicationId: " + JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");

		// Read the content of the input file
		JavaRDD<String> register = sc.textFile(inputPath);
		JavaRDD<String> stations = sc.textFile(inputPath2);

		// Create the criticality
		JavaPairRDD<String, Double> criticality = register.filter(line -> !line.startsWith("station") &&
				!(line.split("[ \\t]+")[3].equals("0") && line.split("[ \\t]+")[4].equals("0"))).mapToPair(line -> {
					String[] fields = line.split("[ \\t]+");
					String day = DateTool.DayOfTheWeek(fields[1]);
					String hour = fields[2].split(":")[0];
					String myKey = fields[0] + " " + day + " " + hour;
					return new Tuple2<>(myKey, new Tuple2<>(Integer.parseInt(fields[4]) == 0 ? 1 : 0, 1));
				}).reduceByKey((x1, x2) -> new Tuple2<>(x1._1 + x2._1, x1._2 + x2._2))
				.filter(y -> (double) y._2._1 / y._2._2 >= threshold)
				.mapToPair(y -> new Tuple2<>(y._1, (double) y._2._1 / y._2._2));

		// Save the criticality
		criticality.coalesce(1).saveAsTextFile(outputFolder + "/criticality");

		// Create the most critical timeslots
		JavaPairRDD<String, Double> mostCriticalTimeslots = criticality
				.mapToPair(tuple -> new Tuple2<>(tuple._1.split(" ")[0], tuple))
				.reduceByKey((tuple1, tuple2) -> {
					// Confronta le criticità e restituisci la tupla con la criticità maggiore
					if (Integer.parseInt(tuple1._1.split(" ")[2]) < Integer.parseInt(tuple2._1.split(" ")[2]))
						return tuple1;
					else if (Integer.parseInt(tuple1._1.split(" ")[2]) == Integer.parseInt(tuple2._1.split(" ")[2])) {
						if (tuple1._1.split(" ")[1].compareTo(tuple2._1.split(" ")[1]) < 0)
							return tuple1;
					}
					return tuple2;
				})
				.mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2));

		// Create a new PairRDD of most critical timeslot station with
		// key = stationId,
		// value = (DayOfTheWeek-Hour, Criticality)
		JavaPairRDD<String, Tuple2<String, Double>> stationTimeslotCrit = mostCriticalTimeslots
				.mapToPair(x -> {
					// (2_Sat_02,402)
					String[] fields = x._1().split(" ");
					String stationId = fields[0];
					String dayWeek = fields[1];
					String hour = fields[2];

					Double crit = x._2();
					return new Tuple2<String, Tuple2<String, Double>>(stationId,
							new Tuple2<String, Double>(dayWeek + " " + hour, crit));
				});

		// Save the most critical timeslots
		stationTimeslotCrit.coalesce(1).saveAsTextFile(outputFolder + "/mostcritical");

		// Create a variable with the coordinates of the stations
		JavaPairRDD<String, String> stationCoordinates = stations
				.filter(line -> !line.startsWith("id"))
				.mapToPair(line -> {
					String[] fields = line.split("\\t");
					String stationId = fields[0];
					String longitude = fields[1];
					String latitude = fields[2];
					return new Tuple2<String, String>(stationId, longitude + "," + latitude);
				});

		stationCoordinates.coalesce(1).saveAsTextFile(outputFolder + "/stcoord");

		// Join the locations with the "critical" stations
		JavaPairRDD<String, Tuple2< Tuple2<String, Double>, String>> resultLocations = stationTimeslotCrit.join(stationCoordinates);

		resultLocations.coalesce(1).saveAsTextFile(outputFolder + "/resultloc");

		// Create a string containing the description of a marker, in the KML
		// format, for each sensor and the associated information
		JavaRDD<String> resultKML = resultLocations
				.map((Tuple2<String, Tuple2< Tuple2<String, Double>, String>> StationMax) -> {

					String stationId = StationMax._1();

					Double critic = StationMax._2()._1()._2();
					String timeslot = StationMax._2()._1()._1();
					String ts[]= timeslot.split(" ");

					String coordinates = StationMax._2()._2();

					String result = "<Placemark><name>" + stationId + "</name>" + "<ExtendedData>"
							+ "<Data name=\"DayWeek\"><value>" + ts[0] + "</value></Data>"
							+ "<Data name=\"Hour\"><value>" + ts[1] + "</value></Data>"
							+ "<Data name=\"Criticality\"><value>" + critic + "</value></Data>"
							+ "</ExtendedData>" + "<Point>" + "<coordinates>" + coordinates + "</coordinates>"
							+ "</Point>" + "</Placemark>";

					return result;
				});

		// There is at most one string for each station.
		// Invoke coalesce(1) to store all data inside one single partition
		resultKML.coalesce(1).saveAsTextFile(outputFolder+"/FINAL_RESULT");

		sc.close();
	}
}