package edu.carleton.comp4601.algorithms;

import java.util.ArrayList;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.mutable.WrappedArray;

public class LDAExample {

	private static MyMongoDB myMongo = new MyMongoDB();
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("JavaLDAExample")
				.master("local")
				.getOrCreate();
		
		// Loads data.
		Dataset<Row> dataset = spark.read().format("libsvm")
				.load("src/output.txt");

		// Trains a LDA model.
		// Uses K topics
		// Runs the algorithm for 10 iterations
		LDA lda = new LDA().setK(2).setMaxIter(10);
		LDAModel model = lda.fit(dataset);
		
		double ll = model.logLikelihood(dataset);
		double lp = model.logPerplexity(dataset);
		System.out.println("The lower bound on the log likelihood of the entire corpus: " + ll);
		System.out.println("The upper bound on perplexity: " + lp);

		// Describe topics.
		// Only want to output the n top-weighted terms
		Dataset<Row> topics = model.describeTopics(3);
		System.out.println("The topics described by their top-weighted terms:");
//		topics.show(true);
		ArrayList<String> topicArr = new ArrayList<String>();
		topics.foreach(
				(ForeachFunction<Row>) row -> {
					System.out.println(row.get(0));
					System.out.println(row.get(1));
					WrappedArray<Integer> test = (WrappedArray<Integer>) row.get(1);
					Integer[] topicAr = (Integer[]) test.array();
					String term = myMongo.getTerm(topicAr[0]);
//					String combinedTerms =  term1+"-"+term2+"-"+term3;
					topicArr.add(term);
					
					// roundabout way of getting counter
					myMongo.addTopic(topicArr.indexOf(term), term);


				}
		);
		// Shows the result.
		System.out.println("Get topics for each file");
		Dataset<Row> transformed = model.transform(dataset);
//		transformed.show(true);
		transformed.foreach(
				(ForeachFunction<Row>) row -> {
					System.out.println(row.get(0));

					DenseVector a = (DenseVector) row.get(2);
					double[] arr = a.toArray();
					
					double max = -1;
					int index = 0;
					for (int i = 0; i < arr.length; i++) {
						if (arr[i] > max) {
							max = arr[i];
							index = i;
						}
					}
					
//					System.out.println("Index max "+ index);
					String topic = myMongo.getTopic(index);
					Double topicID = (double) row.get(0);
					int topicInt = topicID.intValue();
					myMongo.updateFile(topicInt, topic);
					
					System.out.println("Top topic for this file : " + topic);	
					// update mongodb

				}
		);
		spark.stop();
	}

}
