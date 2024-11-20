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
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.regression.RandomForestRegressor;

public final class RandomForestBirdsCorrelation {

    public static void trainRandomForest(SparkSession spark, String inputData, String outputPath) throws IOException {
        Dataset<Row> data = spark
                            .read()
                            .format("csv")
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .load(inputData);
        data.show();
        VectorAssembler assembler = new VectorAssembler()
                                    .setInputCols(new String[]{"bird_population"})
                                    .setOutputCol("features");

        StringIndexer labelIndexer = new StringIndexer()
                                     .setInputCol("bird_flight_ratio")
                                     .setOutputCol("indexedLabel");

        RandomForestRegressor randomForest = new RandomForestRegressor()
                                              .setLabelCol("indexedLabel")
                                              .setFeaturesCol("features")
                                              .setNumTrees(10)
                                              .setMaxDepth(5);

        Pipeline pipe = new Pipeline()
                        .setStages(new PipelineStage[]{assembler, labelIndexer, randomForest});

        Dataset<Row>[] splits = data.randomSplit(new double[]{0.8, 0.2}, 1338);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        PipelineModel model = pipe.fit(trainingData);

        Dataset<Row> predictions = model.transform(testData);
        predictions.select("airport_id", "bird_population", "bird_flight_ratio", "prediction").show();

        RegressionEvaluator evaluator = new RegressionEvaluator()
                                            .setLabelCol("bird_flight_ratio")
                                            .setPredictionCol("prediction")
                                            .setMetricName("rmse");
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE): " + rmse);

        model.write().overwrite().save(outputPath);
        System.out.println("Model saved at: " + outputPath);
    }
    
    public static void main(String[] args) throws Exception {
        /*
            spark-submit --class org.classifiers.RandomForestBirds --master spark://augusta.cs.colostate.edu:30176 \
            target/FlightDataProcessing-1.0-SNAPSHOT.jar <input-csv> /435_TP/output-random-forest
         */
        if (args.length != 2) {
            System.err.println("Usage: RandomForestBirds <input-path> <output-path>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("RandomForestBirds").master("yarn")
                .getOrCreate();

        trainRandomForest(spark, args[0], args[1]);
        spark.stop();
    }
}