package com.atguigu.com;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;


public class Ubounded_wordcount {
    public static void main(String[] args) throws Exception {
        //1,创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2,设置并行度为1
        env.setParallelism(1);
        //3.从端口(无界)读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //4将读过来的数据按照空格切分,取出每一个单词
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception{
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });


        //5,将每个单词组成一个Tulpe2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value)throws Exception{
                return Tuple2.of(value,1);
            }
        });

        //6,将每个相同的单词聚合到一起
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);
        //7,做累加操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

       env.execute();

    }

}
