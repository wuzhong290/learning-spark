package com.oreilly.learningsparkexamples.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;
/**
 * Created by wuzhong on 2017/4/7.
 */
public class Word2Vec1 {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaBookExample").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlCtx = new SQLContext(sc);
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
                RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
                RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        DataFrame documentDF = sqlCtx.createDataFrame(data, schema);

// Learn a mapping from words to Vectors.
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                .setVectorSize(3)
                .setMinCount(0);
        Word2VecModel model = word2Vec.fit(documentDF);
        Encoder<Row> encoder = Encoders.bean(Row.class);
        Dataset<Row> result = model.transform(documentDF).as(encoder);
        for (Row r : result.takeAsList(3)) {
            System.out.println(r);
        }
    }
}
