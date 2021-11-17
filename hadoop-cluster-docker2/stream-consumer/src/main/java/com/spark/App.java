package com.spark;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import scala.Tuple2;

public class App {
    public static final String FedsKafkaTopic = "feds";

    public static final String Keyspace = "feds";
    public static final String AgenciesMostFSsTable = "agencies_most_flight_segs";
    public static final String SpeedStatisticTable = "speed_stat_table";


    public static void main(String[] args) 
    {
        System.out.println("Stream consumer starting");


        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME_IN_SECONDS");
        if (initialSleepTime != null && !initialSleepTime.equals("")) 
        {
            int sleep = Integer.parseInt(initialSleepTime);
            System.out.println("Sleeping on start " + sleep + "sec");
            try {
                Thread.sleep(sleep * 1000);
            } 
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }
        String kafkaUrl = System.getenv("KAFKA_URL");
        if (kafkaUrl == null || kafkaUrl.equals("")) {
            throw new IllegalStateException("KAFKA_URL environment variable must be set");
        }
        String cassandraUrl = System.getenv("CASSANDRA_URL");
        if (cassandraUrl == null || cassandraUrl.equals("")) {
            throw new IllegalStateException("CASSANDRA_URL environment variable must be set");
        }
        String cassandraPortStr = System.getenv("CASSANDRA_PORT");
        if (cassandraPortStr == null || cassandraPortStr.equals("")) {
            throw new IllegalStateException("CASSANDRA_PORT environment variable must be set");
        }
        Integer cassandraPort = Integer.parseInt(cassandraPortStr);
        String dataReceivingTimeInSec = System.getenv("DATA_RECEIVING_TIME_IN_SECONDS");
        if (dataReceivingTimeInSec == null || dataReceivingTimeInSec.equals("")) {
            throw new IllegalStateException("DATA_RECEIVING_TIME_IN_SECONDS environment variable must be set");
        }
        int dataReceivingTime = Integer.parseInt(dataReceivingTimeInSec);

        String agency = System.getenv("FLIGHT_AGENCY");
        if (agency == null || agency.equals("")) {
            throw new IllegalStateException("FLIGHT_AGENCY environment variable must be set.");
        }
        
        String flightId = System.getenv("FLIGHT_ID");
        if (flightId == null || flightId.equals("")) {
            throw new IllegalStateException("FLIGHT_ID environment variable must be set.");
        }
        
        System.out.println("Consumer started");
        prepareCassandraKeyspace(cassandraUrl, cassandraPort);

        SparkConf conf = new SparkConf().setAppName("Feds Spark Streaming").setMaster(sparkMasterUrl);
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(dataReceivingTime * 1000));
        
        streamingContext.sparkContext().setLogLevel("DEBUG");
        System.out.println("Spark started");
       

        Map<String, Object> kafkaParams = getKafkaParams(kafkaUrl);
        Collection<String> topics = Collections.singletonList(FedsKafkaTopic);
        
        JavaInputDStream<ConsumerRecord<Object, String>> stream = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<String> receivedData = stream.map(ConsumerRecord::value);
        System.out.println("*RECEIVED DATA*");
        receivedData.print();
        
        JavaDStream<FedsData> feds = receivedData.map((line) -> {
        	FedsData data = null;
            try {
                data = FedsData.createFedsDataFromLine(line);
            } catch (java.text.ParseException e) {
                System.out.println("Error: " + e.getMessage());
            }
            return data;
        }).filter((data) -> data != null);
               
        JavaDStream<FedsData> flightsWithSegmentId = feds.filter(fed -> {
            return fed.flight_id.equals(flightId); 
        });

 	JavaDStream<FedsData> flightsWithAgency = feds.filter(fed -> {
            return fed.agency.equals(agency); 
        });

        feds.foreachRDD((fedRdd) -> {
            Long count = fedRdd.count();
            if (count <= 0) {
                System.out.println("Empty RDD, skipping");
                return;
            }
            else
                System.out.println("Not empty RDD, count: " + count) ;

            Map<String, Integer> byAgencyCount = fedRdd
                    .mapToPair((ta) -> new Tuple2<String, Integer>(ta.agency, 1))
                    .reduceByKey((a, b) -> a + b)
                    .collectAsMap();

            System.out.println("Segments by agency");
            byAgencyCount.forEach((key, value) -> System.out.println(key + ":" + value));
            
            Iterator<String> iter =byAgencyCount.keySet().iterator();
            while(iter.hasNext()) {
                String c = iter.next();
                System.out.println("Agency name: "+c+", number of flight segments: "+byAgencyCount.get(c));
            }
            
            Tuple2<String, Integer> agenciesWithMostFSs = findAgenciesWithMostFlightSegs(byAgencyCount);          
            
            System.out.println("Agencies with most flight segments");
            System.out.println(agenciesWithMostFSs.toString());
            saveAgenciesWithMostFlightSegments(agenciesWithMostFSs, cassandraUrl, cassandraPort);
        });

            flightsWithAgency.foreachRDD((fedRdd) -> {
            Long count = fedRdd.count();
            if (count <= 0) {
                System.out.println("Empty RDD, skipping");
                return;
            }

            System.out.println("Speed of aircraft for flight segment " + flightId +":");
            
            JavaRDD<Integer> speeds = fedRdd.map((a) -> a.speed);
            
            Integer speedSum = speeds.reduce((speed, acc) -> {
                return speed + acc;
            });
            Double averageSpeed = Double.valueOf(speedSum) / count;
            
            Integer min=speeds.reduce((a, b)-> {
            	Integer minRes;
            	if(a<b)
            		minRes=a;
            	else
            		minRes=b;
            	return minRes;
            });
            
            Integer max=speeds.reduce((a, b)-> {
            	Integer maxRes;
            	if(a>b)
            		maxRes=a;
            	else
            		maxRes=b;
            	return maxRes;
            });
                        
            System.out.println("Speed statistics");
            saveSpeedStatistic(flightId, min, max, averageSpeed, count.intValue(), cassandraUrl, cassandraPort);
        });

        streamingContext.start();
        
        try {
            streamingContext.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Tuple2<String, Integer> findAgenciesWithMostFlightSegs(Map<String, Integer> byAgencyCount) 
    {
        Collection<Integer> flightSegs = byAgencyCount.values();
        final Integer maxFSCount = flightSegs.isEmpty() ? 0 : Collections.max(flightSegs);

        Set<String> keys = byAgencyCount.keySet();
        keys.removeIf((k) -> byAgencyCount.get(k) != maxFSCount);
        
        String result = String.join(",", String.valueOf(keys));
        return new Tuple2<String, Integer>(result, maxFSCount);
    }

    private static Map<String, Object> getKafkaParams(String kafkaUrl) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        kafkaParams.put(ConsumerConfig.CLIENT_ID_CONFIG, "streaming-consumer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "streaming-consumer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return kafkaParams;
    }

    private static void saveSpeedStatistic(String fs, Integer min, Integer max, Double avg, Integer count, String addr, Integer port) {
        CassandraConnector conn = CassandraConnector.getInstance();
        conn.connect(addr, port);

        CqlSession session = conn.getSession();
        RegularInsert insertInto = QueryBuilder
            .insertInto(Keyspace, SpeedStatisticTable)
            .value("flight_segment", QueryBuilder.bindMarker())
            .value("max_speed", QueryBuilder.bindMarker())
            .value("min_speed", QueryBuilder.bindMarker())
            .value("avg_speed", QueryBuilder.bindMarker())
            .value("num_of_flight_segments", QueryBuilder.bindMarker());

        SimpleStatement insertStatement = insertInto.build();
        PreparedStatement preapredStatement = session.prepare(insertStatement);

        BoundStatement boundStatement = preapredStatement.bind()
            .setString(0,fs)
            .setInt(1, max)
            .setInt(2, min)
            .setDouble(3, avg)
            .setInt(4, count);

        session.execute(boundStatement);
        conn.close();
    }

    public static void saveAgenciesWithMostFlightSegments(Tuple2<String, Integer> agenciesWithMostFSs, String addr, Integer port) 
    {
        CassandraConnector conn = CassandraConnector.getInstance();
        conn.connect(addr, port);

        CqlSession session = conn.getSession();
        RegularInsert insertInto = QueryBuilder
            .insertInto(Keyspace, AgenciesMostFSsTable )
            .value("agency_most_flight_segs", QueryBuilder.bindMarker())
            .value("num_of_flight_segs", QueryBuilder.bindMarker());

        SimpleStatement insertStatement = insertInto.build();
        PreparedStatement preapredStatement = session.prepare(insertStatement);

        BoundStatement boundStatement = preapredStatement.bind()
            .setString(0, agenciesWithMostFSs._1)
            .setInt(1, agenciesWithMostFSs._2);
        
        session.execute(boundStatement);
        
        session.close();
        conn.close();
    }
    
     private static void prepareCassandraKeyspace(String addr, Integer port) {
        CassandraConnector conn = CassandraConnector.getInstance();
        conn.connect(addr, port);
        CqlSession session = conn.getSession();

        String createKeyspaceCQL = String.format(
                                        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication "
                                        + "= {'class':'SimpleStrategy', 'replication_factor':1};", 
                                        Keyspace);

        String createSpeedStatistic = String.format(
                                        "CREATE TABLE IF NOT EXISTS %s.%s ("
                                        + " flight_segment text PRIMARY KEY,"
                                        + " max_speed int,"
                                        + " min_speed int,"
                                        + " avg_speed double,"
                                        + " num_of_flight_segments int );", 
                                        Keyspace, SpeedStatisticTable);
                                        
        String citiesAccidents = String.format(
                                        "CREATE TABLE IF NOT EXISTS %s.%s ("
                                        + " agency_most_flight_segs text PRIMARY KEY,"
                                        + " num_of_flight_segs int );",
                                        Keyspace, AgenciesMostFSsTable);

        System.out.println("Preparing Cassandra Keyspace");

        session.execute(createKeyspaceCQL);
        System.out.println("Keyspace created");
               
        session.execute(createSpeedStatistic);
        System.out.println(String.format("Table %s created", SpeedStatisticTable));
        session.execute(citiesAccidents);
        System.out.println(String.format("Table %s created", AgenciesMostFSsTable));

        session.close();
        conn.close();
    }
}





