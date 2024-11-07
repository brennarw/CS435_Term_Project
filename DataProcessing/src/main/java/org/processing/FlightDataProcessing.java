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
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.time.ZoneId;

import com.luckycatlabs.sunrisesunset.SunriseSunsetCalculator;
import com.luckycatlabs.sunrisesunset.dto.Location;

public final class FlightDataProcessing {

    public static void processFlightData(SparkSession spark, String flightDataInputPath, String airportDataInputPath, String timezoneInputPath){
        //Generate new flight data
        //FlightDate, Airline, origin, destination, cancelled, diverted, CRSDepTime (expected dep time), DepTime (actual: hhmm), DepDelayMinutes, DepDelay ....
        JavaRDD<String> flights = spark.read().textFile(flightDataInputPath).javaRDD();
        JavaRDD<String> airports = spark.read().textFile(airportDataInputPath).javaRDD();
        JavaPairRDD<String, String> timezonesRDD = spark.read().textFile(timezoneInputPath).javaRDD()
                                                .mapToPair(tuple -> new Tuple2<>(tuple.split(",", -1)[0], tuple.split(",", -1)[1]));
        List<Tuple2<String, String>> zoneList = timezonesRDD.collect();
        HashMap<String, String> timezones = new HashMap<>();
        for (Tuple2<String, String> zone : zoneList) {
           //System.out.println("beans" + zone._1 + " " + zone._2.replace("\"", ""));
            timezones.put(zone._1.replace("\"", ""), zone._2.replace("\"", ""));
        }
        timezones.remove("iata_code");
        Broadcast<HashMap<String, String>> broadcastMap = spark.sparkContext().broadcast(timezones, scala.reflect.ClassTag$.MODULE$.apply(HashMap.class));
        System.out.println("beans " + broadcastMap.value().keySet());
        String flightHeader = flights.first();
        String airportHeader = airports.first();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        //index 7 hhmm
        JavaPairRDD<String, String> airportsPair = airports
                                                .filter(row -> !row.equals(airportHeader))
                                                .mapToPair(tuple -> new Tuple2<>(tuple.split(",", -1)[13], tuple));
        JavaPairRDD<String,Tuple2<String,String>>  arrivalsPair = flights
                                                .mapToPair(tuple -> new Tuple2<>(tuple.split(",", -1)[2], tuple))
                                                .filter(row -> !row._2.equals(flightHeader))
                                                .filter(date -> {
                                                    Date flightDate = sdf.parse(date._2.split(",", -1)[0]);
                                                    String year = date._2.split(",", -1)[0].substring(0, 4);
                                                    return flightDate.after(sdf.parse(year + "-08-01")) && flightDate.before(sdf.parse(year +"-11-30"));
                                                })
                                                .join(airportsPair);
        JavaPairRDD<String,Tuple2<String,String>>  departuresPair = flights
                                                .mapToPair(tuple -> new Tuple2<>(tuple.split(",", -1)[3], tuple))
                                                .filter(row -> !row._2.equals(flightHeader))
                                                .filter(date -> {
                                                    Date flightDate = sdf.parse(date._2.split(",", -1)[0]);
                                                    String year = date._2.split(",", -1)[0].substring(0, 4);
                                                    return flightDate.after(sdf.parse(year + "-08-01")) && flightDate.before(sdf.parse(year +"-11-30"));
                                                })
                                                .join(airportsPair);
        JavaPairRDD<String, Tuple2<String, String>> sunsetRDD = departuresPair.union(arrivalsPair)
                                                .mapToPair(tuple -> {
                                                    //System.out.println("beans" + tuple._1 + " " + tuple._2 + " " + tuple._2._1 + " " + tuple._2._2);
                                                    String[] splits = tuple._2._2.toString().split(",", -1);
                                                    String lat = splits[4];
                                                    String lon = splits[5];
                                                    Location location = new Location(lat, lon);
                                                    SunriseSunsetCalculator calculator = new SunriseSunsetCalculator(location,  TimezoneMapper.latLngToTimezoneString(Double.parseDouble(lat), Double.parseDouble(lon)));
                                                    Date date = sdf.parse(tuple._2._1.split(",", -1)[0]);
                                                    Calendar flightDate = Calendar.getInstance();
                                                    flightDate.setTime(date);
                                                    String sunset = calculator.getOfficialSunsetForDate(flightDate);
                                                    SimpleDateFormat militartyFormat = new SimpleDateFormat("HH:mm");
                                                    // Date flightTime = 
                                                    // Date sunsetTime = 
                                                    return new Tuple2<>(tuple._1, new Tuple2<>(sunset, sunset));
                                                });
                                
        sunsetRDD.map(tuple -> tuple._1 + ": " + tuple._2).coalesce(1).saveAsTextFile("/435_TP/combinedArrivals");
        departuresPair.map(tuple -> tuple._1 + ": " + tuple._2).coalesce(1).saveAsTextFile("/435_TP/combinedDepartures");

       // System.out.println("Beans " + flightsJoined.collect());
        //fall is august (start) - november (end) -> month 8 - 11
        //filter out days that aren't considered fall 
       // System.out.println(flightsPair.collect());
        // JavaPairRDD<String, String> fallFlights = flightsPair.filter(flight -> {
        //     try{
        //         String[] attributes = flight._1().split(",");
    
        //         SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    
        //         if(!attributes[0].equals("FlightDate")){
        //             Date flightDate = formatter.parse(attributes[0]);
        //             int month = flightDate.getMonth(); //is indexed at 0 - august (7), november (10)
        //             return month >= 7 && month <= 10;
        //         }
        //     } catch(ParseException e){
        //         e.printStackTrace();
        //     }
        //     return false;
        // });
        
        // fallFlights.coalesce(1).saveAsTextFile("/435_TP/preprocessed_flight_data");
    }

    
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Need to input a hadoop file path.");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("FlightDataProcessing").master("local")
                .getOrCreate();

        processFlightData(spark, args[0], args[1], args[2]);
        spark.stop();
    }
}