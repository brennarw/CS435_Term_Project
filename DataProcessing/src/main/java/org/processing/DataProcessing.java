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

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.text.SimpleDateFormat;
import java.util.Comparator;

import com.luckycatlabs.sunrisesunset.SunriseSunsetCalculator;
import com.luckycatlabs.sunrisesunset.dto.Location;

public final class DataProcessing {

    public static void processFlightData(SparkSession spark, String flightDataInputPath, String airportDataInputPath, String birdStrikeDataPath){

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //Generate new flight data
        //FlightDate, Airline, origin, destination, cancelled, diverted, CRSDepTime (expected dep time), DepTime (actual: hhmm), DepDelayMinutes, DepDelay ....
        JavaRDD<String> flights = spark.read().textFile(flightDataInputPath).javaRDD(); //read in flights
        JavaRDD<String> airports = spark.read().textFile(airportDataInputPath).javaRDD(); //read in airports
        JavaRDD<String> strikes = spark.read().textFile(birdStrikeDataPath).javaRDD();
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
        String birdStrikePerFlightOutputDir = "/435_TP/birdStrikePerFlight";
        String topNOutputDir = "/435_TP/topNAirports";
        String bottomNOutputDir = "/435_TP/bottomNAirports";
        String topAndBottomN = "/435_TP/topAndBottomN";
        sunsetRDD.map(tuple -> tuple._1 + ": " + tuple._2).coalesce(1).saveAsTextFile(sunsetOutputDir);
        JavaRDD<String> headerRDD = sc.parallelize(Collections.singletonList("airportId,latitude_deg,longitude_deg,flightCount"));
        ;

        // Get flight frequencies & save to file
        //FORMAT: JavaPairRDD<<Airport Code, Latiude, Longitude>, <Count>>
        JavaPairRDD<String, Integer> flightFreqenciesRDD = sunsetRDD
                                                        .mapToPair(tuple -> {
                                                            String[] airportVal = tuple._2._2.split(",", -1);
                                                            return new Tuple2<>(tuple._1 + "," + airportVal[4] + "," + airportVal[5], 1);
                                                        })
                                                        .reduceByKey((x, y) -> x + y)
                                                        .coalesce(1);
        headerRDD.union(flightFreqenciesRDD.map(tuple -> tuple._1 + "," + tuple._2)).saveAsTextFile(flightFrequenciesOutputDir);        
        
        //FORMAT: JavaRDD<Airport Code, Latiude, Longitude, Ratio (bird strikes/flight count)>
        JavaRDD<String> BirdJoinedFlights = flightFreqenciesRDD
                                                    .mapToPair(tuple -> new Tuple2<>(tuple._1.substring(0, tuple._1.indexOf(",")), new Tuple2<>(tuple._1, tuple._2)))
                                                    .join(
                                                        strikes
                                                        .mapToPair(line -> new Tuple2<>(line.substring(1, line.indexOf(",")), line.substring(line.lastIndexOf(",") + 1))))
                                                    .map(tuple -> {
                                                        double ratio = Double.parseDouble(tuple._2._2) / (double)tuple._2._1._2;
                                                        return tuple._2._1._1 + "," + ratio;
                                                    });

        BirdJoinedFlights.saveAsTextFile(birdStrikePerFlightOutputDir);

        int n = 50;
        List<Tuple2<Integer, String>> topNAirportsSwapped = flightFreqenciesRDD
                                                                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                                                                .sortByKey(false)
                                                                .take(n);
        //FORMAT: JavaPairRDD<<Airport Code, Latiude, Longitude>, <Count>>
        JavaPairRDD<String, Integer> topNAirportsRDD = sc.parallelizePairs(
            topNAirportsSwapped.stream()
            .map(tuple -> new Tuple2<>(tuple._2, tuple._1)).collect(Collectors.toList())
            );
        
        //FORMAT: JavaRDD<Airport Code, Latiude, Longitude, Count>
        JavaRDD<String> topOutput = headerRDD.union(topNAirportsRDD.map(tuple -> tuple._1 + "," + tuple._2));
        topOutput.coalesce(1).saveAsTextFile(topNOutputDir);
        // Gather bottom N airports by number of flights
        List<Tuple2<Integer, String>> bottomNAirportsSwapped = flightFreqenciesRDD
                                                                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                                                                .sortByKey(true)
                                                                .take(n);
        //FORMAT: JavaPairRDD<<Airport Code, Latiude, Longitude>, <Count>>
        JavaPairRDD<String, Integer> bottomNAirportsRDD = sc.parallelizePairs(
            bottomNAirportsSwapped.stream().map(tuple -> new Tuple2<>(tuple._2, tuple._1)).collect(Collectors.toList())
        );
        //FORMAT: JavaRDD<Airport Code, Latiude, Longitude, Count>
        JavaRDD<String> bottomOutput = headerRDD.union(bottomNAirportsRDD.map(tuple -> tuple._1 + "," + tuple._2));
        bottomOutput.coalesce(1).saveAsTextFile(bottomNOutputDir);
        //FORMAT: JavaRDD<Airport Code, Latiude, Longitude, Count>
        JavaRDD<String> bottomTopOutput = headerRDD.union(topNAirportsRDD.map(tuple -> tuple._1 + "," + tuple._2)).union(bottomNAirportsRDD.map(tuple -> tuple._1 + "," + tuple._2));
        bottomTopOutput.coalesce(1).saveAsTextFile(topAndBottomN);
    }

    public static void processBirdStrikeData(SparkSession spark, String birdStrikeInputPath, String sunsetOutputPath) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        
        JavaRDD<String> birdStrikes = spark.read().textFile(birdStrikeInputPath).javaRDD(); //read in bird strike dataset

        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

        //FORMAT: JavaRDD<BirdStrikeData>
        //"INDEX_NR,INCIDENT_DATE,INCIDENT_MONTH,INCIDENT_YEAR,TIME,TIME_OF_DAY,AIRPORT_ID,AIRPORT,AIRPORT_LATITUDE,AIRPORT_LONGITUDE,RUNWAY,STATE,FAAREGION,LOCATION,ENROUTE_STATE,OPID,OPERATOR,REG,FLT,AIRCRAFT,AMA,AMO,EMA,EMO,AC_CLASS,AC_MASS,TYPE_ENG,NUM_ENGS,ENG_1_POS,ENG_2_POS,ENG_3_POS,ENG_4_POS,PHASE_OF_FLIGHT,HEIGHT,SPEED,DISTANCE,SKY,PRECIPITATION,AOS,COST_REPAIRS,COST_OTHER,COST_REPAIRS_INFL_ADJ,COST_OTHER_INFL_ADJ,INGESTED_OTHER,INDICATED_DAMAGE,DAMAGE_LEVEL,STR_RAD,DAM_RAD,STR_WINDSHLD,DAM_WINDSHLD,STR_NOSE,DAM_NOSE,STR_ENG1,DAM_ENG1,ING_ENG1,STR_ENG2,DAM_ENG2,ING_ENG2,STR_ENG3,DAM_ENG3,ING_ENG3,STR_ENG4,DAM_ENG4,ING_ENG4,STR_PROP,DAM_PROP,STR_WING_ROT,DAM_WING_ROT,STR_FUSE,DAM_FUSE,STR_LG,DAM_LG,STR_TAIL,DAM_TAIL,STR_LGHTS,DAM_LGHTS,STR_OTHER,DAM_OTHER,OTHER_SPECIFY,EFFECT,EFFECT_OTHER,BIRD_BAND_NUMBER,SPECIES_ID,SPECIES,REMARKS,REMAINS_COLLECTED,REMAINS_SENT,WARNED,NUM_SEEN,NUM_STRUCK,SIZE,NR_INJURIES,NR_FATALITIES,COMMENTS,REPORTED_NAME,REPORTED_TITLE,SOURCE,PERSON,LUPDATE,TRANSFER"
        JavaRDD<String> fallStrikes = birdStrikes.filter(strike -> {
            String[] attributes = strike.split(",", -1);
            int year = Integer.parseInt(attributes[3]);
            int month = Integer.parseInt(attributes[2]);
            return (year >= 2018 && year <= 2020) && (month >= 8 && month <= 11);
        }).filter(tuple -> {
            String[] attributes = tuple.split(",", -1);
            if(attributes[4].replace("\"", "").trim().isEmpty() || attributes[8].replace("\"", "").trim().isEmpty() || attributes[9].replace("\"", "").trim().isEmpty()){
                return false;
            } else {
                try{
                    double tempLat = Double.parseDouble(attributes[8].replace("\"", "").trim());
                    double tempLon = Double.parseDouble(attributes[9].replace("\"", "").trim());
                    return true;
                } catch (Exception e) {
                    System.out.println("BEANS2: lat: " + attributes[8] + " lon: " + attributes[9]);
                    return false;
                }
            }
        });
        //FORMAT: JavaRDD<BirdStrikeData>
        //NOTE: same as fallStrikes but filtered for sunset
        //"INDEX_NR,INCIDENT_DATE,INCIDENT_MONTH,INCIDENT_YEAR,TIME,TIME_OF_DAY,AIRPORT_ID,AIRPORT,AIRPORT_LATITUDE,AIRPORT_LONGITUDE,RUNWAY,STATE,FAAREGION,LOCATION,ENROUTE_STATE,OPID,OPERATOR,REG,FLT,AIRCRAFT,AMA,AMO,EMA,EMO,AC_CLASS,AC_MASS,TYPE_ENG,NUM_ENGS,ENG_1_POS,ENG_2_POS,ENG_3_POS,ENG_4_POS,PHASE_OF_FLIGHT,HEIGHT,SPEED,DISTANCE,SKY,PRECIPITATION,AOS,COST_REPAIRS,COST_OTHER,COST_REPAIRS_INFL_ADJ,COST_OTHER_INFL_ADJ,INGESTED_OTHER,INDICATED_DAMAGE,DAMAGE_LEVEL,STR_RAD,DAM_RAD,STR_WINDSHLD,DAM_WINDSHLD,STR_NOSE,DAM_NOSE,STR_ENG1,DAM_ENG1,ING_ENG1,STR_ENG2,DAM_ENG2,ING_ENG2,STR_ENG3,DAM_ENG3,ING_ENG3,STR_ENG4,DAM_ENG4,ING_ENG4,STR_PROP,DAM_PROP,STR_WING_ROT,DAM_WING_ROT,STR_FUSE,DAM_FUSE,STR_LG,DAM_LG,STR_TAIL,DAM_TAIL,STR_LGHTS,DAM_LGHTS,STR_OTHER,DAM_OTHER,OTHER_SPECIFY,EFFECT,EFFECT_OTHER,BIRD_BAND_NUMBER,SPECIES_ID,SPECIES,REMARKS,REMAINS_COLLECTED,REMAINS_SENT,WARNED,NUM_SEEN,NUM_STRUCK,SIZE,NR_INJURIES,NR_FATALITIES,COMMENTS,REPORTED_NAME,REPORTED_TITLE,SOURCE,PERSON,LUPDATE,TRANSFER"
        JavaRDD<String> sunsetStrikes = fallStrikes.filter(tuple -> {
            String[] attributes = tuple.split(",", -1);
            if(attributes[4].replace("\"", "").trim().isEmpty() || attributes[8].replace("\"", "").trim().isEmpty() || attributes[9].replace("\"", "").trim().isEmpty()){
                return false;
            } 
            String latitude = attributes[8].replace("\"", "").trim();
            String longitude = attributes[9].replace("\"", "").trim();
            Location location = new Location(latitude, longitude);
            SunriseSunsetCalculator calculator = new SunriseSunsetCalculator(location, TimezoneMapper.latLngToTimezoneString(Double.parseDouble(latitude), Double.parseDouble(longitude)));
            Date currentDate = sdf.parse(attributes[1]);
            Calendar strikeDate = Calendar.getInstance();
            strikeDate.setTime(currentDate);
            String sunset = calculator.getOfficialSunsetForDate(strikeDate);
            SimpleDateFormat militaryFormat = new SimpleDateFormat("HH:mm");
            String timeString = attributes[4].replace("\"", "").trim(); //in the formate: "HH:mm"
            Date strikeTime = militaryFormat.parse(timeString);
            Date sunsetTime = militaryFormat.parse(sunset);
            //grab a 30 minute padding on either side of the sunset time
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sunsetTime);
            calendar.add(Calendar.MINUTE, 30);
            Date sunsetLate = calendar.getTime();
            calendar.add(Calendar.MINUTE, -60);
            Date sunsetEarly = calendar.getTime();
            if(strikeTime.after(sunsetEarly) && strikeTime.before(sunsetLate)){
                return true; 
            }
            return false; 

        });

        //make a key for the sunset data: <"airportID,lat,lon", 1>
        JavaPairRDD<String, Integer> idSunsetRDD = sunsetStrikes.mapToPair(strike -> {
            String[] attributes = strike.split(",", -1);
            String airportID = attributes[6].replace("\"", "");
            double lat = Double.parseDouble(attributes[8].replace("\"", "").trim());
            double lon = Double.parseDouble(attributes[9].replace("\"", "").trim());
            String key = airportID + "," + lat + "," + lon;
            return new Tuple2<>(key, 1);
        });

        //reduce by key to get the number of strikes per airport
        JavaRDD<String> strikeCountRDD = idSunsetRDD.reduceByKey((x,y) -> x + y).map(tuple -> {
            return tuple._1 + "," + tuple._2;
        });

        // JavaRDD<String> fullHeaderRDD = sc.parallelize(Arrays.asList("INDEX_NR,INCIDENT_DATE,INCIDENT_MONTH,INCIDENT_YEAR,TIME,TIME_OF_DAY,AIRPORT_ID,AIRPORT,AIRPORT_LATITUDE,AIRPORT_LONGITUDE,RUNWAY,STATE,FAAREGION,LOCATION,ENROUTE_STATE,OPID,OPERATOR,REG,FLT,AIRCRAFT,AMA,AMO,EMA,EMO,AC_CLASS,AC_MASS,TYPE_ENG,NUM_ENGS,ENG_1_POS,ENG_2_POS,ENG_3_POS,ENG_4_POS,PHASE_OF_FLIGHT,HEIGHT,SPEED,DISTANCE,SKY,PRECIPITATION,AOS,COST_REPAIRS,COST_OTHER,COST_REPAIRS_INFL_ADJ,COST_OTHER_INFL_ADJ,INGESTED_OTHER,INDICATED_DAMAGE,DAMAGE_LEVEL,STR_RAD,DAM_RAD,STR_WINDSHLD,DAM_WINDSHLD,STR_NOSE,DAM_NOSE,STR_ENG1,DAM_ENG1,ING_ENG1,STR_ENG2,DAM_ENG2,ING_ENG2,STR_ENG3,DAM_ENG3,ING_ENG3,STR_ENG4,DAM_ENG4,ING_ENG4,STR_PROP,DAM_PROP,STR_WING_ROT,DAM_WING_ROT,STR_FUSE,DAM_FUSE,STR_LG,DAM_LG,STR_TAIL,DAM_TAIL,STR_LGHTS,DAM_LGHTS,STR_OTHER,DAM_OTHER,OTHER_SPECIFY,EFFECT,EFFECT_OTHER,BIRD_BAND_NUMBER,SPECIES_ID,SPECIES,REMARKS,REMAINS_COLLECTED,REMAINS_SENT,WARNED,NUM_SEEN,NUM_STRUCK,SIZE,NR_INJURIES,NR_FATALITIES,COMMENTS,REPORTED_NAME,REPORTED_TITLE,SOURCE,PERSON,LUPDATE,TRANSFER"));
        JavaRDD<String> reduceHeaderRDD = sc.parallelize(Arrays.asList("airport_id,latitude_deg,longitude_deg,strike_count"));
        JavaRDD<String> strikeSunsetCountResultRDD = reduceHeaderRDD.union(strikeCountRDD);
        strikeSunsetCountResultRDD.coalesce(1).saveAsTextFile(sunsetOutputPath);
        sc.close();
    }

    
    public static void main(String[] args) throws Exception {
        /*
        TO RUN: inside of /DataProcessing/ compile with
            mvn clean package
        then run the code with 
            spark-submit --class org.processing.DataProcessing --master spark://<your spark port> \
            --jars target/DataProcessing-1.0-SNAPSHOT.jar sunrise-jar/SunriseSunsetCalculator-1.3-SNAPSHOT.jar \
            <path to flight data in HDFS> <path to airport data in HDFS> <path to bird strike in HDFS>
        */

        if (args.length < 2) {
            System.err.println("Usage: Need to input a hadoop file path.");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("FlightDataProcessing").master("yarn")
                .getOrCreate();

        //NOTE: have to uncomment whichever processor you want to use!!
        processFlightData(spark, args[0], args[1], args[2]);
        // processBirdStrikeData(spark, args[0], args[1]); //ARGUMENTS: <bird_strike_data> <sunset_output> <fall_output>

        /*
            in brenna's cluster: 

            spark-submit --class org.processing.DataProcessing --master spark://bogota.cs.colostate.edu:30282 --jars target/Data
Processing-1.0-SNAPSHOT.jar sunrise-jar/SunriseSunsetCalculator-1.3-SNAPSHOT.jar /435_TP/bird_strike_data /435_TP/2018_sunset_output
            */
        spark.stop();
    }
}