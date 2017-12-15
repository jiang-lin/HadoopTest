package com.jianglin.spark;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2017/12/6.
 */
public class SparkMongodbTest {
    private final static Logger logger = LoggerFactory.getLogger(SparkMongodbTest.class);
    private static String DEFAULT_AUTH_DB = "vphone";

    private static String seed1         = "192.168.0.77:27017";
    private static String seed2         = "192.168.0.77:27017";
//    private static String username      = "root";
//    private static String password      = "123456";
    private static String replSetName   = "callings";

    private static String authURIPrefix = "mongodb://" +
//            username + ":" + password + "@" +
//            seed1 + "," + seed2 + "/";
           seed1 + "/";
    private static String authURISuffix = "?replicaSet=" + replSetName;
    private static String inputColl     = "vphone.vg_sync_app_calling";
    private static String outputColl    = "testdb.output";

    private static String mongoURI      = authURIPrefix + DEFAULT_AUTH_DB + authURISuffix;
    private static String inputURI      = authURIPrefix + inputColl + authURISuffix;
    private static String outputURI     = authURIPrefix + outputColl + authURISuffix;

    public static void main(String[] args) {
        SparkConf sparkConf =new SparkConf().setAppName("Spark WordCount written by java").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
      /*  JavaSparkContext sc= new JavaSparkContext("local", "HBaseTest");*/
        Configuration config = new Configuration();
        config.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
        config.set("mongo.job.output.format", "com.mongodb.hadoop.MongoOutputFormat");
//        config.set("mongo.auth.uri", mongoURI);
        config.set("mongo.input.uri", "mongodb://192.168.0.77:27017/vphone.vg_sync_app_calling");
        config.set("mongo.output.uri", "mongodb://192.168.0.77:27017/vphone.test1");
        JavaPairRDD<Object,BSONObject> documents = sc.newAPIHadoopRDD(
                config,                        // Configuration
                MongoInputFormat.class,   // InputFormat: read from a live cluster.
                Object.class,             // Key class
                BSONObject.class          // Value class
        );

        JavaPairRDD<Object,BSONObject> document1 = documents.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Object, BSONObject> objectBSONObjectTuple2) throws Exception {
                return objectBSONObjectTuple2._2().get("subaccount").equals("854704560848158720");
            }
        });
        JavaPairRDD<String,Integer> pairs=document1.mapToPair(new PairFunction<Tuple2<Object, BSONObject>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Object, BSONObject> objectBSONObjectTuple2) throws Exception {
                BSONObject bSONObject = objectBSONObjectTuple2._2();
                logger.info("----------------"+bSONObject.toString());
                System.out.println(objectBSONObjectTuple2.productIterator());
                return new Tuple2<String, Integer>(bSONObject.get("otherSideNumber")==null?"":bSONObject.get("otherSideNumber").toString(),1);
            }
        });
        JavaPairRDD<String,Integer> wordsCount =pairs.reduceByKey(new Function2<Integer, Integer, Integer>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                logger.info(v1+v2+"");
                return v1+v2;
            }
        });
        wordsCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> pairs) throws Exception {
                // TODO Auto-generated method stub
                logger.info(pairs._1+" : " +pairs._2);
            }
        });

        wordsCount.saveAsNewAPIHadoopFile(
                "file://this-is-completely-unused",
                Object.class,
                BSONObject.class,
                MongoOutputFormat.class,
                config
        );








    }
}


