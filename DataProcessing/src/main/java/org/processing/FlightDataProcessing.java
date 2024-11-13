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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.text.SimpleDateFormat;
import java.util.Comparator;

import com.luckycatlabs.sunrisesunset.SunriseSunsetCalculator;
import com.luckycatlabs.sunrisesunset.dto.Location;

public final class FlightDataProcessing {

    public static void processFlightData(SparkSession spark, String flightDataInputPath, String airportDataInputPath){

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //Generate new flight data
        //FlightDate, Airline, origin, destination, cancelled, diverted, CRSDepTime (expected dep time), DepTime (actual: hhmm), DepDelayMinutes, DepDelay ....
        JavaRDD<String> flights = spark.read().textFile(flightDataInputPath).javaRDD(); //read in flights
        JavaRDD<String> airports = spark.read().textFile(airportDataInputPath).javaRDD(); //read in airports
        String flightHeader = flights.first();
        String airportHeader = airports.first();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        //FORMAT: JavaPairRDD<<Airport Code>, <Airport Data>>
        JavaPairRDD<String, String> airportsPair = airports
                                                .filter(row -> !row.equals(airportHeader)) //filter out column names
                                                .mapToPair(tuple -> new Tuple2<>(tuple.split(",", -1)[13].replace("\"", ""), tuple)) //map airport codes (ex. DEN) from ind 13 as the key
                                                .filter(row -> !row._1.equals("")); //filter out missing airport codes
        //FORMAT: JavaPairRDD<<Airport Code>, Tuple2<Flight Data, Airport Data>>
        JavaPairRDD<String,Tuple2<String,String>>  originPair = flights
                                                .filter(row -> !row.equals(flightHeader)) //filter out column names
                                                .mapToPair(tuple -> new Tuple2<>(tuple.split(",", -1)[2], tuple)) //map airport codes (ex. DEN) for aorigin airport from ind 2 as the key
                                                .filter(date -> {
                                                    Date flightDate = sdf.parse(date._2.split(",", -1)[0]); //find date of flight
                                                    String year = date._2.split(",", -1)[0].substring(0, 4); //get year
                                                    return flightDate.after(sdf.parse(year + "-08-01")) && flightDate.before(sdf.parse(year +"-11-30")); //filter for fall (Aug 1 - Nov 30) only for years in the flight data (2018-2020)
                                                })
                                                .join(airportsPair)
                                                .filter(tuple -> {
                                                    try{
                                                        Integer.valueOf(tuple._2._1.split(",", -1)[7].substring(0 , tuple._2._1.split(",", -1)[7].length()-2)); //attempt to grab the hour:minute of departure at ind 7, filter out if non-numeric
                                                        return true;
                                                    }
                                                    catch(Exception e){
                                                        return false;
                                                    }
                                                } )
                                                .filter(tuple -> {
                                                    String[] splits = tuple._2._2.split(",", -1);
                                                    String lat = splits[4]; //from airports dataset
                                                    String lon = splits[5]; //from airports dataset
                                                    Location location = new Location(lat, lon); //set location
                                                    //generate sunset calculator
                                                    SunriseSunsetCalculator calculator = new SunriseSunsetCalculator(location,  TimezoneMapper.latLngToTimezoneString(Double.parseDouble(lat), Double.parseDouble(lon)));
                                                    Date date = sdf.parse(tuple._2._1.split(",", -1)[0]);
                                                    Calendar flightDate = Calendar.getInstance();
                                                    flightDate.setTime(date);
                                                    String sunset = calculator.getOfficialSunsetForDate(flightDate); //get sunset
                                                    SimpleDateFormat militartyFormat = new SimpleDateFormat("HH:mm");
                                                    String dateString = tuple._2._1.split(",", -1)[7].substring(0 , tuple._2._1.split(",", -1)[7].length()-2); //get hour:minute of departure from flight date
                                                    int hour = Integer.valueOf(dateString) / 100; //calculate hour
                                                    int minute = Integer.valueOf(dateString) % 100; //calculate minute
                                                    Date flightTime = militartyFormat.parse(hour + ":" + minute); //format to time object
                                                    Date sunsetTime = militartyFormat.parse(sunset); //fortmat sunset to time object
                                                    //Add range for grabbing flights (currently 30 min before and after sunset)
                                                    Calendar calendar = Calendar.getInstance();
                                                    calendar.setTime(sunsetTime);
                                                    calendar.add(Calendar.MINUTE, 30);
                                                    Date sunsetLate = calendar.getTime();
                                                    calendar.add(Calendar.MINUTE, -60);
                                                    Date sunsetEarly = calendar.getTime();
                                                    if(flightTime.after(sunsetEarly) && flightTime.before(sunsetLate)){
                                                        return true; //if flight departure is within range, keep record
                                                    }
                                                    return false;  //flight departure is not in range, discard record
                                                });
        //FORMAT: JavaPairRDD<<Airport Code>, Tuple2<Flight Data, Airport Data>>
        JavaPairRDD<String,Tuple2<String,String>>  destPair = flights
                                                .filter(row -> !row.equals(flightHeader)) //filter out column names
                                                .mapToPair(tuple -> new Tuple2<>(tuple.split(",", -1)[3], tuple))  //map airport codes (ex. DEN) for destination airport from ind 3 as the key
                                                .filter(date -> {
                                                    Date flightDate = sdf.parse(date._2.split(",", -1)[0]); //find date of flight
                                                    String year = date._2.split(",", -1)[0].substring(0, 4); //get year
                                                    return flightDate.after(sdf.parse(year + "-08-01")) && flightDate.before(sdf.parse(year +"-11-30")); //filter for fall (Aug 1 - Nov 30) only for years in the flight data (2018-2020)
                                                })
                                                .join(airportsPair)
                                                .filter(tuple -> {
                                                    try{
                                                        Integer.valueOf(tuple._2._1.split(",", -1)[10].substring(0 , tuple._2._1.split(",", -1)[10].length()-2)); //attempt to grab the hour:minute of departure at ind 7, filter out if non-numeric
                                                        return true;
                                                    }
                                                    catch(Exception e){
                                                        return false;
                                                    }
                                                } )
                                                .filter(tuple -> {
                                                    String[] splits = tuple._2._2.split(",", -1);
                                                    String lat = splits[4]; //from airports dataset
                                                    String lon = splits[5]; //from airports dataset
                                                    Location location = new Location(lat, lon); //set location
                                                    //generate sunset calculator
                                                    SunriseSunsetCalculator calculator = new SunriseSunsetCalculator(location,  TimezoneMapper.latLngToTimezoneString(Double.parseDouble(lat), Double.parseDouble(lon)));
                                                    Date date = sdf.parse(tuple._2._1.split(",", -1)[0]);
                                                    Calendar flightDate = Calendar.getInstance();
                                                    flightDate.setTime(date);
                                                    String sunset = calculator.getOfficialSunsetForDate(flightDate); //get sunset
                                                    SimpleDateFormat militartyFormat = new SimpleDateFormat("HH:mm");
                                                    String dateString = tuple._2._1.split(",", -1)[10].substring(0 , tuple._2._1.split(",", -1)[10].length()-2); //get hour:minute of departure from flight date
                                                    int hour = Integer.valueOf(dateString) / 100; //calculate hour
                                                    int minute = Integer.valueOf(dateString) % 100; //calculate minute
                                                    Date flightTime = militartyFormat.parse(hour + ":" + minute); //format to time object
                                                    Date sunsetTime = militartyFormat.parse(sunset); //fortmat sunset to time object
                                                    //Add range for grabbing flights (currently 30 min before and after sunset)
                                                    Calendar calendar = Calendar.getInstance();
                                                    calendar.setTime(sunsetTime);
                                                    calendar.add(Calendar.MINUTE, 30);
                                                    Date sunsetLate = calendar.getTime();
                                                    calendar.add(Calendar.MINUTE, -60);
                                                    Date sunsetEarly = calendar.getTime();
                                                    if(flightTime.after(sunsetEarly) && flightTime.before(sunsetLate)){
                                                        return true; //if flight arrival is within range, keep record
                                                    }
                                                    return false;  //flight arrival is not in range, discard record
                                                });
        //FORMAT: JavaPairRDD<<Airport Code>, Tuple2<Flight Data, Airport Data>>
        JavaPairRDD<String, Tuple2<String, String>> sunsetRDD = destPair.union(originPair); //combine origin and destination RDDs without any reduce by key                 
        //Write to file  
        String sunsetOutputDir = "/435_TP/sunsetFlights";
        String flightFrequenciesOutputDir = "/435_TP/flightFrequencies";
        String topNOutputDir = "/435_TP/topNAirports";
        String bottomNOutputDir = "/435_TP/bottomNAirports";
        sunsetRDD.map(tuple -> tuple._1 + ": " + tuple._2).coalesce(1).saveAsTextFile(sunsetOutputDir);

        // Get flight frequencies & save to file
        JavaPairRDD<String, Integer> flightFreqenciesRDD = sunsetRDD
                                                        .mapToPair(tuple -> new Tuple2<>(tuple._1, 1))
                                                        .reduceByKey((x, y) -> x + y)
                                                        .coalesce(1);
        flightFreqenciesRDD.saveAsTextFile(flightFrequenciesOutputDir);
        // Gather top N airports by number of flights
        int n = 10;
        List<Tuple2<Integer, String>> topNAirportsSwapped = flightFreqenciesRDD
                                                                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                                                                .sortByKey(false)
                                                                .take(n);
        JavaPairRDD<String, Integer> topNAirportsRDD = sc.parallelizePairs(
            topNAirportsSwapped.stream().map(tuple -> new Tuple2<>(tuple._2, tuple._1)).collect(Collectors.toList())
        );
        // Gather bottom N airports by number of flights
        List<Tuple2<Integer, String>> bottomNAirportsSwapped = flightFreqenciesRDD
                                                                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                                                                .sortByKey(true)
                                                                .take(n);
        JavaPairRDD<String, Integer> bottomNAirportsRDD = sc.parallelizePairs(
            bottomNAirportsSwapped.stream().map(tuple -> new Tuple2<>(tuple._2, tuple._1)).collect(Collectors.toList())
        );
        topNAirportsRDD.coalesce(1).saveAsTextFile(topNOutputDir);
        bottomNAirportsRDD.coalesce(1).saveAsTextFile(bottomNOutputDir);
    }

    
    public static void main(String[] args) throws Exception {
        /*
        TO RUN: inside of /DataProcessing/ compile with
            mvn clean package
        then run the code with 
            spark-submit --class org.processing.FlightDataProcessing --master spark://<your spark port> \
            --jars target/FlightDataProcessing-1.0-SNAPSHOT.jar sunrise-jar/SunriseSunsetCalculator-1.3-SNAPSHOT.jar \
            <path to flight data in HDFS> <path to airport data in HDFS>
        */

        if (args.length < 2) {
            System.err.println("Usage: Need to input a hadoop file path.");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("FlightDataProcessing").master("yarn")
                .getOrCreate();

        processFlightData(spark, args[0], args[1]);
        spark.stop();
    }
}