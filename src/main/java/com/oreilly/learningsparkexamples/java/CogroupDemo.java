/**
 * Illustrates a wordcount in Java
 */
package com.oreilly.learningsparkexamples.java;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class CogroupDemo {
    public static void main(String[] args) throws Exception {
        String master = "local[3]";
        JavaSparkContext sc = new JavaSparkContext(
                master, "CogroupDemo");
        JavaPairRDD<Integer, String> names=sc.parallelizePairs(Arrays.asList(
                new Tuple2(1,"Spark"),
                new Tuple2(2,"Hadoop"),
                new Tuple2(3,"Kylin"),
                new Tuple2(4,"Flink")));
        List<Tuple2<Integer,String>> s2 = new ArrayList<>();
        s2.add(new Tuple2(1,"String"));
        s2.add(new Tuple2(2,"int"));
        s2.add(new Tuple2(3,"byte"));
        s2.add(new Tuple2(4,"bollean"));
        s2.add(new Tuple2(5,"float"));
        s2.add(new Tuple2(1,"34"));
        s2.add(new Tuple2(1,"45"));
        s2.add(new Tuple2(2,"47"));
        s2.add(new Tuple2(3,"75"));
        s2.add(new Tuple2(4,"95"));
        s2.add(new Tuple2(5,"16"));
        s2.add(new Tuple2(1,"85"));
        JavaPairRDD<Integer,String> types=sc.parallelizePairs(s2);
        JavaPairRDD<Integer, Tuple2<Iterable<String>,Iterable<String>>> rets = names.cogroup(types);
        JavaPairRDD<Integer,String> types2 = types.foldByKey("", new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1+":"+v2;
            }
        });
        JavaPairRDD<Integer,String> types3 = types.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1+":"+v2;
            }
        });
        System.out.println(JSON.toJSONString(rets.collect()));
        System.out.println(JSON.toJSONString(types2.collect()));
        System.out.println(JSON.toJSONString(types3.collect()));
        JavaPairRDD<Integer,String> combineByKeyRDD = types.combineByKey(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1 + " :createCombiner: ";
            }
        }, new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + " :mergeValue: " + v2;
            }
        }, new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + " :mergeCombiners: " + v2;
            }
        });
        System.out.println(JSON.toJSONString(combineByKeyRDD.collect()));
        JavaPairRDD<Integer,List<String>> combineByKeyRDD1 = types.combineByKey(new Function<String, List<String>>() {
            @Override
            public List<String> call(String v1) throws Exception {
                List<String> rets = Lists.newArrayList();
                rets.add(v1);
                return rets;
            }
        }, new Function2<List<String>, String, List<String>>() {
            @Override
            public List<String> call(List<String> v1, String v2) throws Exception {
                v1.add(v2);
                return v1;
            }
        }, new Function2<List<String>, List<String>, List<String>>() {
            @Override
            public List<String> call(List<String> v1, List<String> v2) throws Exception {
                v1.addAll(v2);
                return v1;
            }
        });
        System.out.println("combineByKeyRDD1:"+JSON.toJSONString(combineByKeyRDD1.collect()));
        JavaPairRDD<Integer,Iterable<String>> groupByKeyRDD = types.groupByKey();
        System.out.println("groupByKeyRDD:"+JSON.toJSONString(groupByKeyRDD.collect()));
    }
}
