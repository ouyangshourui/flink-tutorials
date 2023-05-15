package com.flink.tutorials.java.chapter4_api.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * 构造条TelnetWatermarkExample代码的测试数据，格式为:随机单词|时间戳，时间戳为近3分的时间戳，写入到一个文件中,时间戳到毫秒
 * import random
 * import time
 *
 * # 文件路径
 * file_path = "testdata.txt"
 *
 * # 生成测试数据并写入文件
 * with open(file_path, "w") as file:
 *     current_timestamp = int(time.time() * 1000)
 *     start_timestamp = current_timestamp - 3 * 60 * 1000  # 当前时间往前推3分钟的毫秒数
 *
 *     for _ in range(1000):
 *         word = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=5))  # 生成长度为5的随机单词
 *         timestamp = random.randint(start_timestamp, current_timestamp)  # 随机生成近3分钟内的时间戳（以毫秒为单位）
 *         data = f"{word}|{timestamp}\n"
 *         file.write(data)
 */

public class TelnetWatermarkExample {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间特性为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建Telnet数据流
        DataStream<String> telnetDataStream = env.socketTextStream("localhost", 9999);
       // telnetDataStream.print();

        // 解析数据并提取时间戳
        DataStream<Tuple2<String, Long>> parsedDataStream = telnetDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                // 解析数据格式为 "<data>|<timestamp>"
                String[] tokens = value.split("\\|");
                if (tokens.length == 2) {
                    String data = tokens[0];
                    long timestamp = Long.parseLong(tokens[1]);
                    out.collect(new Tuple2<>(data, timestamp));
                }
            }
        });
        //parsedDataStream.print();

        // 分配时间戳和水印
        DataStream<Tuple2<String, Long>> withTimestampsAndWatermarks = parsedDataStream
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                    private static final long serialVersionUID = 1L;
                    private long currentMaxTimestamp = 0L;
                    private final long maxOutOfOrderness = 5000L; // 最大允许的乱序时间

                    @Override
                    public Watermark getCurrentWatermark() {
                        // 设置水印为当前最大时间戳减去乱序时间
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                        // 提取元素中的时间戳字段
                        long timestamp = element.f1;
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }
                });

        // 打印数据流和水印标记
        DataStream testStream = withTimestampsAndWatermarks.process(new WatermarkPrintingProcessFunction());

        testStream.print();
        // 执行任务
        env.execute("Telnet Watermark Example");
    }
}

class WatermarkPrintingProcessFunction extends ProcessFunction<Tuple2<String, Long>, String> {

    @Override
    public void processElement(Tuple2<String, Long> input, Context context, Collector<String> out) throws Exception {
        // 获取当前水印时间戳


        // 格式化为字符串
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");



        long watermarkTimestamp = context.timerService().currentWatermark();
        String slocal = input.getField(0) +"|"+ Long.toString(input.getField(1));
        // 打印水印时间戳
        Instant instant = Instant.ofEpochMilli(watermarkTimestamp);
        LocalDateTime currentTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        String currentDateTime = currentTime.format(formatter);

        Instant instant1= Instant.ofEpochMilli(input.getField(1));
        LocalDateTime currentTime1 = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        String currentDateTime1 = currentTime.format(formatter);

        System.out.println(slocal+":"+currentDateTime1+":Watermark Timestamp: " + currentDateTime);

        // 处理数据并输出结果
        // ...

        // 如果需要基于水印时间戳注册定时器
        // context.timerService().registerEventTimeTimer(...);
    }
}
