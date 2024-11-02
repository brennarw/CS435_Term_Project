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

package org.processing;

import scala.Tuple2;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.text.ParseException;

public final class FlightDataProcessing {

    public static void processFlightData(SparkSession spark, String flightDataInputPath){
        //Generate new flight data
        //FlightDate, Airline, origin, destination, cancelled, diverted, CRSDepTime (expected dep time), DepTime (actual: hhmm), DepDelayMinutes, DepDelay ....
        JavaRDD<String> flights = spark.read().textFile(flightDataInputPath).javaRDD();

        //fall is august (start) - november (end) -> month 8 - 11
        //filter out days that aren't considered fall 
        JavaRDD<String> fallFlights = flights.filter(flight -> {
            try{
                String[] attributes = flight.split(",");
    
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    
                if(!attributes[0].equals("FlightDate")){
                    Date flightDate = formatter.parse(attributes[0]);
                    int month = flightDate.getMonth(); //is indexed at 0 - august (7), november (10)
                    return month >= 7 && month <= 10;
                }
            } catch(ParseException e){
                e.printStackTrace();
            }
            return false;
        });
        
        fallFlights.coalesce(1).saveAsTextFile("/435_TP/preprocessed_flight_data");
    }

    
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: Need to input a hadoop file path.");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("FlightDataProcessing").master("yarn")
                .getOrCreate();

        processFlightData(spark, "/435_TP/flight_data");
        spark.stop();
    }
}