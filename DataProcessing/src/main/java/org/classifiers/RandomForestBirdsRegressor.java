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
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.param.ParamMap;
import java.util.Arrays;
import java.util.List;

public class RandomForestBirdsRegressor {
    
    public static void trainRandomForestRegressor(SparkSession spark, String inputData, String outputPath) {
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


            data = data.na().drop();

            String[] featureCols = {"bird_population", "flight"};

            // Assemble features into a single vector
            VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features");

            // Define the target variable
            String targetCol = "bird_flight_ratio";

            RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol(targetCol)
                .setFeaturesCol("features")
                .setNumTrees(100)
                .setMaxDepth(5)
                .setMaxBins(32)
                .setSeed(42);

            // Create Pipeline
            Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{assembler, rf});

            // Create parameter grid for cross-validation
            ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(rf.numTrees(), new int[]{50, 100, 150})
                .addGrid(rf.maxDepth(), new int[]{5, 10, 15})
                .build();

            RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol(targetCol)
                .setPredictionCol("prediction")
                .setMetricName("rmse"); 

            CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(5) 
                .setSeed(42);

            Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3}, 42);
            Dataset<Row> trainingData = splits[0];
            Dataset<Row> testData = splits[1];

            System.out.println("\n=== Training Information ===");
            System.out.println("Training set size: " + trainingData.count());
            System.out.println("Test set size: " + testData.count());

            System.out.println("\nTraining Random Forest Regressor with cross-validation...");
            CrossValidatorModel cvModel = cv.fit(trainingData);

            Dataset<Row> predictions = cvModel.transform(testData);

            double rmse = evaluator.setMetricName("rmse").evaluate(predictions);
            double mse = evaluator.setMetricName("mse").evaluate(predictions);
            double mae = evaluator.setMetricName("mae").evaluate(predictions);
            double r2 = evaluator.setMetricName("r2").evaluate(predictions);

            System.out.println("\n=== Model Performance Metrics ===");
            System.out.println("Root Mean Squared Error (RMSE): " + String.format("%.6f", rmse));
            System.out.println("Mean Squared Error (MSE): " + String.format("%.6f", mse));
            System.out.println("Mean Absolute Error (MAE): " + String.format("%.6f", mae));
            System.out.println("R² Score: " + String.format("%.6f", r2));

            String modelPath = outputPath + "_trained_regressor_model";
            System.out.println("\nSaving trained model to: " + modelPath);
            PipelineModel bestModel = (PipelineModel) cvModel.bestModel();
            bestModel.write().overwrite().save(modelPath);


            System.out.println("\n=== Residuals (Prediction Errors) ===");
            predictions.withColumn("residual", col(targetCol).minus(col("prediction")))
                .select("prediction", targetCol, "residual")
                .show(5);

            String predictionsPath = outputPath + "_predictions";
            predictions
                .select("airport_id","bird_population", "flight_count", targetCol, "prediction")
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
            System.err.println("Usage: RandomForestBirdsRegressor <input-path> <output-path>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("RandomForestBirdsRegressor")
            .master("yarn")
            .getOrCreate();

        trainRandomForestRegressor(spark, args[0], args[1]);
        spark.stop();
    }

    //spark-submit --class org.classifiers.RandomForestBirdsRegressor --master yarn target/FlightDataProcessing-1.0-SNAPSHOT-jar-with-dependencies.jar /user/aaron16/input2/new_training_data.csv /user/aaron16/output/random-forest-regressor-model
}
