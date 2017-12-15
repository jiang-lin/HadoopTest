package com.jianglin.spark;

import org.apache.spark.api.java.function.Function;
import org.bson.BSONObject;

/**
 * Created by admin on 2017/12/6.
 */
public class MongoDBMapFunction implements Function<BSONObject, BSONObject> {
    public BSONObject call(BSONObject bsonObject) throws Exception {
        System.out.println(bsonObject);
        bsonObject.put("sign", "132456778889990");
        return bsonObject;
    }
}
