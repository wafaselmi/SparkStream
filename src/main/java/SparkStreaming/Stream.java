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

public class Stream {
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

    public static double distance(double lat1, double lon1, double lat2, double lon2) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;
        dist = dist * 1.609344 ;

        return (dist);
    }

    private static double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }

    private static double vitesse(double distance /*km*/, double time /*ms*/) {
        return (distance /  time) *60 *60 * 1000;
    }

    public static void aadRowToHBase(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table table1 = connection.getTable(TableName.valueOf("cars_data"));

        try {
            Put p = new Put(Bytes.toBytes(args[0]+','+args[1]));
            p.addColumn(Bytes.toBytes("route"), Bytes.toBytes("routeId"),Bytes.toBytes(args[0]));
            p.addColumn(Bytes.toBytes("route"), Bytes.toBytes("carId"),Bytes.toBytes(args[1]));
            p.addColumn(Bytes.toBytes("car"), Bytes.toBytes("lat"),Bytes.toBytes(args[3]));
            p.addColumn(Bytes.toBytes("car"), Bytes.toBytes("long"),Bytes.toBytes(args[4]));
            p.addColumn(Bytes.toBytes("car"), Bytes.toBytes("time"),Bytes.toBytes(args[5]));
            p.addColumn(Bytes.toBytes("car"), Bytes.toBytes("speed"),Bytes.toBytes(args[6]));
            table1.put(p);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            table1.close();
            connection.close();
        }
    }

    public static void main(String[] args) throws InterruptedException,IOException {

        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table table1 = connection.getTable(TableName.valueOf("cars_data"));
        Table table2 = connection.getTable(TableName.valueOf("routes_data"));

        SparkConf conf = new SparkConf()
                .setAppName("NetworkTraffic");
        JavaStreamingContext jssc =
                new JavaStreamingContext(conf, Durations.seconds(10));



        JavaReceiverInputDStream<String> lines =
                jssc.socketTextStream("192.168.1.169", 9999);


        // sessionId,route,x,y,time(millisends) => [sessionId,route,x,y,time]
        JavaDStream<String[]> seperatedLines = lines.map(line -> line.split(","));
        // [sessionId,route,x,y,time] => (sessionId , [sessionId,route,x,y,time,speed = 0, number = 0])
        JavaPairDStream<String,String[]> seperatedSession = seperatedLines.mapToPair(line -> new Tuple2<>(line[0], addX(addX(line,"0"),"1")));

        // (sessionId , [sessionId,route,x,y,time,speed = 0, number = 0]) => (sessionId , [sessionId,route,x,y,time,speed, number = 0])
        JavaPairDStream<String,String[]> calculatedSpeed = seperatedSession.reduceByKey((a,b) -> {
            double distance = distance(Double.parseDouble(a[2]),Double.parseDouble(a[3]),Double.parseDouble(b[2]),Double.parseDouble(b[3]));
            double time = Integer.parseInt(b[4]) - Integer.parseInt(a[4]) ;
            if(Double.parseDouble(a[5]) == 0){
                b[5] = String.valueOf(vitesse(distance,time));
            } else {
                b[5] = String.valueOf((vitesse(distance,time) + Double.parseDouble(a[5])) /2) ;
            }
            return b;
        });

        JavaPairDStream<String,String[]> groupedByRoute = calculatedSpeed.mapToPair(a -> new Tuple2<>(a._2[1], a._2));


        groupedByRoute.foreachRDD(
                rdd -> {
                    rdd.collect().forEach((data)  -> {
                        String[] arg = data._2;
                        try {
                            Put p = new Put(Bytes.toBytes(arg[0]+','+arg[1]));
                            System.out.println(arg[0] + arg[1] +arg[2]+arg[3]+arg[4]+arg[5]);
                            p.addColumn(Bytes.toBytes("route"), Bytes.toBytes("routeId"),Bytes.toBytes(arg[0]));
                            p.addColumn(Bytes.toBytes("route"), Bytes.toBytes("carId"),Bytes.toBytes(arg[1]));
                            p.addColumn(Bytes.toBytes("car"), Bytes.toBytes("lat"),Bytes.toBytes(arg[2]));
                            p.addColumn(Bytes.toBytes("car"), Bytes.toBytes("long"),Bytes.toBytes(arg[3]));
                            p.addColumn(Bytes.toBytes("car"), Bytes.toBytes("time"),Bytes.toBytes(arg[4]));
                            p.addColumn(Bytes.toBytes("car"), Bytes.toBytes("speed"),Bytes.toBytes(arg[5]));
                            table1.put(p);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
        );

        JavaPairDStream<String,String[]> groupedByRouteCalculatedSpeed = groupedByRoute.reduceByKey((a,b) -> {
            a[5] = String.valueOf(Double.parseDouble(a[5]) + Double.parseDouble(b[5]));
            a[6] = String.valueOf(Integer.parseInt(b[6]) + Integer.parseInt(a[6]));
            return a;
        });

        JavaPairDStream<String,String> printed = groupedByRouteCalculatedSpeed.mapToPair(a -> new Tuple2<>(a._1, a._2[6]));

        JavaPairDStream<String,String> groupedByRouteCalculatedSpeedMoy = groupedByRouteCalculatedSpeed.mapToPair(a -> new Tuple2<>(a._1, String.valueOf(Double.parseDouble(a._2[5])/Double.parseDouble(a._2[6]))));

        groupedByRouteCalculatedSpeedMoy.print();

        groupedByRouteCalculatedSpeedMoy.foreachRDD(
                rdd -> {
                    rdd.collect().forEach((data)  -> {
                        try {
                            Put p = new Put(Bytes.toBytes(data._1));
                            p.addColumn(Bytes.toBytes("route"), Bytes.toBytes("speed"),Bytes.toBytes(data._2));
                            table2.put(p);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
        );

        jssc.start();
        jssc.awaitTermination();


        table1.close();
        table2.close();
        connection.close();
    }
}