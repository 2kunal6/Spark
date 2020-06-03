package de.fraunhofer.iais.kd.bda.spark;


import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import de.fraunhofer.iais.kd.bda.spark.Userset;

public class UsersetSpark {
	public static void main(String[] args) {
	//	String inputFile= "/home/livlab/data/last-fm-sample1000000.tsv";
		
		//put here the path to the input
		String inputFile= "/home/kunal/Documents/DSBD/last-fm-sample1000000.tsv";
		String appName = "UsersetSpark";
	
		SparkConf conf  = new SparkConf().setAppName(appName)
										 .setMaster("local[*]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//Read file
		JavaRDD<String> input = context.textFile(inputFile);
		
		//Split lines into words
		JavaRDD<String> words = input.flatMap(line->
		{String[] parts = line.split("DUMMY VALUE");return Arrays.asList(parts).iterator();});
		
		JavaPairRDD<String, String> wordOne = words.mapToPair(word -> 
	{return new Tuple2<String,String>(word.split("\t")[3], word.split("\t")[0]);});

		JavaPairRDD<String, > usersetcount = wordOne.aggregateByKey(
				//aggregator initial value
				new Userset(),
				//how to add a value to aggregator
				(agg, value) -> agg.add(value),
				//how to combine two aggregators
				(agg1,agg2) -> agg1.add(agg2)
				);

		 
		usersetcount.saveAsTextFile("userset");
		context.close();

	}
}
