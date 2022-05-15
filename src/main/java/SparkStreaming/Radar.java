package SparkStreaming;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Radar {
    // Function to add x in arr
    public static String[] addX(String arr[], String x)
    {
        int i;

        // create a new ArrayList
        List<String> arrlist
                = new ArrayList<String>(
                Arrays.asList(arr));

        // Add the new element
        arrlist.add(x);

        // Convert the Arraylist to array
        arr = arrlist.toArray(arr);

        // return the array
        return arr;
    }

    public static double dist(double lat1, double long1, double lat2, double long2) {
        double theta = long1 - long2;
        double distance = Math.sin(radius(lat1)) * Math.sin(radius(lat2)) + Math.cos(radius(lat1)) * Math.cos(radius(lat2)) * Math.cos(radius(theta));
        distance = Math.acos(distance);
        distance = degree(distance);
        distance = distance * 60 * 1.1515;
        distance = distance * 1.609344 ;

        return (distance);
    }

    private static double radius(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private static double degree(double rad) {
        return (rad * 180.0 / Math.PI);
    }

    private static double speed(double dist /*km*/, double time /*ms*/) {
        return (dist /  time) *60 *60 * 1000;
    }


    public static void main(String[] args) throws InterruptedException,IOException {

        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table table1 = connection.getTable(TableName.valueOf("cars_data"));

        SparkConf conf = new SparkConf()
                .setAppName("Radar");
        JavaStreamingContext jssc =
                new JavaStreamingContext(conf, Durations.seconds(15));



        JavaReceiverInputDStream<String> lines =
                jssc.socketTextStream("192.168.1.18", 9999);


        // carId,lat,long,time
        JavaDStream<String[]> seperatedLines = lines.map(line -> line.split(","));
        JavaPairDStream<String,String[]> seperatedSession = seperatedLines.mapToPair(line -> new Tuple2<>(line[0], addX(line,"0")));

        JavaPairDStream<String,String[]> calculatedSpeed = seperatedSession.reduceByKey((a,b) -> {
            double distance = dist(Double.parseDouble(a[1]),Double.parseDouble(a[2]),Double.parseDouble(b[1]),Double.parseDouble(b[2]));
            double time = Integer.parseInt(b[3]) - Integer.parseInt(a[3]) ;
            if(Double.parseDouble(a[4]) == 0){
                b[4] = String.valueOf(speed(distance,time));
            } else {
                b[4] = String.valueOf((speed(distance,time) + Double.parseDouble(a[4])) /2) ;
            }
            return b;
        });



        calculatedSpeed.foreachRDD(
                rdd -> {
                    rdd.collect().forEach((data)  -> {
                        String[] arg = data._2;
                        try {
                            if(Double.parseDouble(arg[4]) >90){


                            Put p = new Put(Bytes.toBytes(arg[0]));
                            p.addColumn(Bytes.toBytes("carInfo"), Bytes.toBytes("carId"),Bytes.toBytes(arg[0]));
                            p.addColumn(Bytes.toBytes("carInfo"), Bytes.toBytes("speed"),Bytes.toBytes(arg[4]));
                            table1.put(p);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    });
                }
        );







        jssc.start();
        jssc.awaitTermination();


        table1.close();
        connection.close();
    }
}