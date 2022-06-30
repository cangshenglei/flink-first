package com.atguigu.com;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.concurrent.ExecutionException;

import static java.lang.System.out;

public class BatchProcessing {
    /**
     * 1.创建sparkConf
     * 2.创建sparkContext
     * 3.读取文件中的数据(textfile) =》RDD （弹性式分布式数据集）
     * 4.使用flatMap算子将文件中的每一行数据按照空格切分切出每一个单词 .flatMap(_.split(" "))
     * 5.使用map并组成Tuple2元组 .map((_,1))
     * 6.使用reduceByKey（1.将相同的单词聚合到一块2.做累加）.reduceByKey(_+_)
     * 7.打印结果
     * 8.关闭spark连接
     */
    public static void main(String[] args) throws Exception {
        //1,创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2,获取文件中的数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");
        //3,将文件中的每一行数据按照空格切分出每一个单词
        FlatMapOperator<String, String> word = dataSource.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word1 : words) {
                out.collect(word1);
            }
        }).returns(Types.STRING);
        //4,将每个单词组成tuple2元组
        MapOperator<String, Tuple2<String, Integer>> wordToOne = word.map(value -> Tuple2.of(value, 1))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                ;
        //5,将相同的单词聚合到一起
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);
        //6,做累加
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);
        //7,将结果打印到控制台
        result.print();

    }
}
