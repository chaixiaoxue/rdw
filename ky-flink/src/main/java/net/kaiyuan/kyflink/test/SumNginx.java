package net.kaiyuan.kyflink.test;

import net.kaiyuan.kyflink.utils.Configs;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @Author: liuxiaoshuai
 * @Date: 2019/10/25
 * @Description:
 */
public class SumNginx {
    public static void main(String[] args) throws   Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //environment.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment,settings);

        Properties properties = Configs.getProperties();

        Kafka kafka_mt_loan = new Kafka().version("universal")
                .topic("nginx_request")
                .startFromEarliest()
                .property("bootstrap.servers", properties.getProperty("bootstrap.servers"))
                .property("group.id", "nginx_request")
                .property("zookeeper.connect", properties.getProperty("zookeeper.connect"))
                .sinkPartitionerFixed();

        streamTableEnvironment.connect(kafka_mt_loan)
                .withFormat(new Json()
                        .failOnMissingField(true)
                        .deriveSchema())
                .withSchema(new Schema()
                        .field("message",Types.STRING)
                )
                .inAppendMode()
                .registerTableSource("nginx_request");

        // where SUBSTRING(message,-6,5) < 1
        String sql4 = "select count(message) as num   from nginx_request  ";
        Table table4 = streamTableEnvironment.sqlQuery(sql4);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = streamTableEnvironment.toRetractStream(table4, Row.class);
        tuple2DataStream.print();

        environment.execute("Flink Table Json Engine");
    }
}
