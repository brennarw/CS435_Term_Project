/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.classifiers;

import scala.Tuple2;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.param.ParamMap;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.functions;
import org.apache.spark.ml.feature.PCA;

public final class RandomForestBirds {

        public static void trainRandomForest(SparkSession spark, String inputData, String outputPath) {
            try {
                // Load data
                Dataset<Row> data = spark
                    .read()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(inputData);
    
                System.out.println("\n=== Dataset Information ===");
                System.out.println("Total records: " + data.count());
                System.out.println("\nSchema:");
                data.printSchema();
                System.out.println("\nSample Data:");
                data.show(5);
    
                // Calculate statistics for bird_flight_ratio
                double[] flightRatioStats = data.stat().approxQuantile("bird_flight_ratio", 
                new double[]{0.33, 0.66}, 0.01); 
                
                System.out.println("\n=== Bird Flight Ratio Statistics ===");
                System.out.println("33rd percentile: " + flightRatioStats[0]);
                System.out.println("66th percentile: " + flightRatioStats[1]);
    
                // Create feature vector
                VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"bird_population"})
                    .setOutputCol("features");
    
                // Create ternary labels based on median flight ratio
                Dataset<Row> processedData = data.withColumn("label",
                when(col("bird_flight_ratio").lt(flightRatioStats[0]), 0.0)
                .when(col("bird_flight_ratio").lt(flightRatioStats[1]), 1.0)
                .otherwise(2.0));

                processedData = processedData.withColumn("bird_population",
                        col("bird_population").plus(rand().multiply(0.1)));

                // Create base random forest classifier
                RandomForestClassifier rf = new RandomForestClassifier()
                    .setLabelCol("label")
                    .setFeaturesCol("features") 
                    .setNumTrees(10)
                    .setMaxDepth(3)
                    .setMaxBins(32)
                    .setSeed(42);
    
                // Create pipeline
                Pipeline pipeline = new Pipeline()
                    .setStages(new PipelineStage[]{assembler, rf});
    
                // Create parameter grid for cross validation
                ParamMap[] paramGrid = new ParamGridBuilder()
                    .addGrid(rf.numTrees(), new int[]{50, 100, 200})
                    .addGrid(rf.maxDepth(), new int[]{5, 10, 15})
                    .build();
    
                // Create evaluator
                MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                    .setLabelCol("label")
                    .setPredictionCol("prediction")
                    .setMetricName("f1");
    
                // Create cross validator
                CrossValidator cv = new CrossValidator()
                        .setEstimator(pipeline)
                        .setEvaluator(evaluator)
                        .setEstimatorParamMaps(paramGrid)
                        .setNumFolds(5)  // Use 5-fold cross validation
                        .setSeed(42);
    
                // Split data
                Dataset<Row>[] splits = processedData.randomSplit(new double[]{0.6, 0.4}, 42);
                Dataset<Row> trainingData = splits[0];
                Dataset<Row> testData = splits[1];
    
                System.out.println("\n=== Training Information ===");
                System.out.println("Training set size: " + trainingData.count());
                System.out.println("Test set size: " + testData.count());
    
                // Train model with cross validation
                System.out.println("\nTraining model with cross validation...");
                CrossValidatorModel cvModel = cv.fit(trainingData);
    
                // Make predictions
                Dataset<Row> predictions = cvModel.transform(testData);
    
                // Calculate various metrics
                MulticlassClassificationEvaluator metricEvaluator = new MulticlassClassificationEvaluator()
                    .setLabelCol("label")
                    .setPredictionCol("prediction");
    
                // Calculate different metrics
                double accuracy = metricEvaluator.setMetricName("accuracy").evaluate(predictions);
                double precision = metricEvaluator.setMetricName("weightedPrecision").evaluate(predictions);
                double recall = metricEvaluator.setMetricName("weightedRecall").evaluate(predictions);
                double f1Score = metricEvaluator.setMetricName("f1").evaluate(predictions);
    
                System.out.println("\n=== Model Performance Metrics ===");
                System.out.println("Accuracy: " + String.format("%.4f", accuracy));
                System.out.println("Precision: " + String.format("%.4f", precision));
                System.out.println("Recall: " + String.format("%.4f", recall));
                System.out.println("F1 Score: " + String.format("%.4f", f1Score));

                String modelPath = outputPath + "_trained_model";
                System.out.println("\nSaving trained model to: " + modelPath);
                PipelineModel bestModel = (PipelineModel) cvModel.bestModel();
                bestModel.write().overwrite().save(modelPath);
    
                // Show prediction distribution
                System.out.println("\n=== Prediction Distribution ===");
                predictions.groupBy("prediction")
                    .count()
                    .orderBy("prediction")
                    .show();
    
                // Show confusion matrix using SQL
                predictions.createOrReplaceTempView("predictions_table");
                System.out.println("\n=== Confusion Matrix ===");
                spark.sql(
                    "SELECT label, prediction, COUNT(*) as count " +
                    "FROM predictions_table " +
                    "GROUP BY label, prediction " +
                    "ORDER BY label, prediction"
                ).show();
    
                // Show sample predictions
                System.out.println("\n=== Sample Predictions ===");
                predictions.select("airport_id", "bird_population", "bird_flight_ratio", "label", "prediction")
                    .orderBy(rand())
                    .show(10);
    
                // Save predictions
                String predictionsPath = outputPath + "_predictions";
                predictions
                    .select("airport_id", "bird_population", "bird_flight_ratio", "label", "prediction")
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(predictionsPath);
                System.out.println("Predictions saved to: " + predictionsPath);
    
            } catch (Exception e) {
                System.err.println("Error in training/prediction process:");
                e.printStackTrace();
            }
        }
    
        public static void main(String[] args) {
            if (args.length != 2) {
                System.err.println("Usage: RandomForestBirds <input-path> <output-path>");
                System.exit(1);
            }
    
            SparkSession spark = SparkSession
                .builder()
                .appName("RandomForestBirds")
                .master("yarn")
                .getOrCreate();
    
            trainRandomForest(spark, args[0], args[1]);
            spark.stop();
        }
        
       // spark-submit   --class org.classifiers.RandomForestBirds   --master spark://kinshasa:30216   target/FlightDataProcessing-1.0-SNAPSHOT-jar-with-dependencies.jar   /user/aaron16/input/training_data.csv   /user/aaron16/output/random-forest-model

    }