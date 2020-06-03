package de.fraunhofer.iais.kd.bda.spark;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import de.fraunhofer.iais.kd.bda.spark.Userset;

public class UsersetSpark {
	public static void main(String[] args) throws IOException {
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

		JavaPairRDD<String, Userset> usersetcount = wordOne.aggregateByKey(
				//aggregator initial value
				new Userset(),
				//how to add a value to aggregator
				(agg, value) -> agg.add(value),
				//how to combine two aggregators
				(agg1,agg2) -> agg1.add(agg2)
				);

		 
		//usersetcount.saveAsTextFile("userset");
		List<String> ans = new ArrayList<String>();
		for (Tuple2<String, Userset> test : usersetcount.collect()) {
			ans.add(test._1 + ", " + test._2.userset);
        }
		FileUtils.writeLines(new File("userset.txt"), "utf-8", ans);
		context.close();

	}
}
