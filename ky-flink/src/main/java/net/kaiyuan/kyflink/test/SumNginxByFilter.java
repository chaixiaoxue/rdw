package net.kaiyuan.kyflink.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.kaiyuan.kyflink.utils.Configs;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: liuxiaoshuai
 * @Date: 2019/10/26
 * @Description:
 */
public class SumNginxByFilter {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.enableCheckpointing(60000);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = Configs.getProperties();
        Properties kafkaPro = Configs.getProperties();
        kafkaPro.setProperty("bootstrap.servers",properties.getProperty("bootstrap.servers"));
        kafkaPro.setProperty("zookeeper.connect",properties.getProperty("zookeeper.connect"));
        kafkaPro.setProperty("group.id","nginx_request_10");
        kafkaPro.setProperty("auto.offset.reset","earliest");

        DataStream<String> dataStream = environment.addSource(new FlinkKafkaConsumer<>("nginx_request", new SimpleStringSchema(), kafkaPro));

        DataStream<String> message = dataStream.rebalance().map(new MapFunction<String, String>() {
            Long sum=0L;
            @Override
            public String map(String value) {
                sum++;
                System.out.println("sum"+sum);
                return value;
            }
        });
        message.print();
        environment.execute();
    }

}
