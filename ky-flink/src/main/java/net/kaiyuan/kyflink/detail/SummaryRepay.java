package net.kaiyuan.kyflink.detail;

import com.alibaba.fastjson.JSONObject;
import net.kaiyuan.kyflink.utils.Configs;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;


/**
 * @Author: liuxiaoshuai
 * @Date: 2019/10/22
 * @Description:
 */
public class SummaryRepay {
    public static void main(String[] args) throws   Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment,settings);
        Properties properties = Configs.getProperties();
        String bootstrap = properties.getProperty("bootstrap.servers");
        String zookeeper = properties.getProperty("zookeeper.connect");
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", bootstrap);
        prop.setProperty("zookeeper.connect", zookeeper);
        prop.setProperty("transaction.max.timeout.ms","50000");
        Kafka kafka_mt_loan = new Kafka().version("universal")
                .topic("dwb_qydproduction_mt_loan")
                .startFromLatest()
                .property("bootstrap.servers", properties.getProperty("bootstrap.servers"))
                .property("group.id", "mt_loan_1")
                .property("zookeeper.connect", properties.getProperty("zookeeper.connect"))
                .sinkPartitionerFixed();

        Kafka kafka_mt_loan_billing = new Kafka().version("universal")
                .topic("dwb_qydproduction_mt_loan_billing")
                .startFromEarliest()
                .property("bootstrap.servers", properties.getProperty("bootstrap.servers"))
                .property("group.id", "mt_loan_billing")
                .property("zookeeper.connect", "172.16.71.145:2181,172.16.71.27:2181,172.16.71.60:2181")
                .sinkPartitionerFixed();

        Kafka kafka_mt_loan_billing_detail = new Kafka().version("universal")
                .topic("dwb_qydproduction_mt_loan_billing_detail")
                .startFromEarliest()
                .property("bootstrap.servers", properties.getProperty("bootstrap.servers"))
                .property("group.id", "mt_loan_billing_detail")
                .property("zookeeper.connect", properties.getProperty("zookeeper.connect"))
                .sinkPartitionerFixed();

        Kafka kafka_new_mt_loan_billing_detail = new Kafka().version("universal")
                .topic("dwb_qydnewproduction_mt_loan_billing_detail")
                .startFromEarliest()
                .property("bootstrap.servers", properties.getProperty("bootstrap.servers"))
                .property("group.id", "new_mt_loan_billing_detail")
                .property("zookeeper.connect", properties.getProperty("zookeeper.connect"))
                .sinkPartitionerFixed();

        //标的表数据
        String[] mt_loan_field =  new String[]{"id","_ums_id_","rowkey","amount","match_amount","price","interest_rate","sponsor_fee","service_fee","third_service_fee","third_user_id","debt_type","audit_type","debit_term","tender_name","status","open_time","close_time","borrower","repay_date","actual_repay_time","project_number","file_number","entrust_user_id","remark","reply","transaction_id","auth_status","create_time","update_time","version","logical_del","product_type"};
        TypeInformation[] mt_loan_array = new TypeInformation[mt_loan_field.length];
        for(int i=0;i<mt_loan_field.length;i++){
            mt_loan_array[i]= Types.ROW_NAMED(
                    new String[]{"nowValue","beforeValue","type","updated"},
                    new TypeInformation[]{Types.STRING,Types.STRING,Types.STRING,Types.BOOLEAN});
        }
        streamTableEnvironment.connect(kafka_mt_loan)
                .withFormat(new Json()
                        .failOnMissingField(true)
                        .deriveSchema())
                .withSchema(new Schema()
                        .field("dbName",Types.STRING)
                        .field("id",Types.STRING)
                        .field("fields", Types.ROW_NAMED(
                                mt_loan_field,
                                mt_loan_array
                        ))
                        .field("ddl",Types.BOOLEAN)
                        .field("tableName",Types.STRING)
                )
                .inAppendMode()
                .registerTableSource("dwb_qydproduction_mt_loan");

        //还贷账单表数据
        String[] mt_loan_billing_field =  new String[]{"id","user_id","account_id","protocol_no","loan_id","repay_date","principal","due_principal","interest","due_interest","due_penalty","due_late_fee","late_fee_last_date","due_amount","repaid_amount","sponsor_user_id","sponsor_account_id","sponsor_protocol_no","sponsor_fee","product_type","type","status","create_time","update_time","version","logical_del","execute_status"};
        TypeInformation[] mt_loan_billing_array = new TypeInformation[mt_loan_billing_field.length];
        for(int i=0;i<mt_loan_billing_field.length;i++){
            mt_loan_billing_array[i]= Types.ROW_NAMED(
                    new String[]{"nowValue","beforeValue","type","updated"},
                    new TypeInformation[]{Types.STRING,Types.STRING,Types.STRING,Types.BOOLEAN});
        }
        streamTableEnvironment.connect(kafka_mt_loan_billing)
                .withFormat(new Json()
                        .failOnMissingField(true)
                        .deriveSchema())
                .withSchema(new Schema()
                        .field("dbName",Types.STRING)
                        .field("id",Types.STRING)
                        .field("fields", Types.ROW_NAMED(
                                mt_loan_billing_field,
                                mt_loan_billing_array
                        ))
                        .field("ddl",Types.BOOLEAN)
                        .field("tableName",Types.STRING)
                )
                .inAppendMode()
                .registerTableSource("dwb_qydproduction_mt_loan_billing");

        //还款账单变更明细表
        String[] mt_loan_billing_detail_field= new String[]{"id","billing_id","user_id","account_id","protocol_no","loan_id","type","split_id","send_time","return_time","due_principal","due_interest","due_penalty","due_late_fee","account_amount","actual_principal","actual_interest","actual_penalty","actual_late_fee","status","moption","create_time","update_time","version","logical_del"};
        TypeInformation[] mt_loan_billing_detail_array = new TypeInformation[mt_loan_billing_detail_field.length];
        for(int i=0;i<mt_loan_billing_detail_field.length;i++){
            mt_loan_billing_detail_array[i]= Types.ROW_NAMED(
                            new String[]{"nowValue","beforeValue","type","updated"},
                            new TypeInformation[]{Types.STRING,Types.STRING,Types.STRING,Types.BOOLEAN});
        }
        streamTableEnvironment.connect(kafka_mt_loan_billing_detail)
                .withFormat(new Json()
                        .failOnMissingField(true)
                        .deriveSchema())
                .withSchema(new Schema()
                        .field("dbName",Types.STRING)
                        .field("id",Types.STRING)
                        .field("fields", Types.ROW_NAMED(
                                mt_loan_billing_detail_field,
                                mt_loan_billing_detail_array
                        ))
                        .field("ddl",Types.BOOLEAN)
                        .field("tableName",Types.STRING)
                )
                .inAppendMode()
                .registerTableSource("dwb_qydproduction_mt_loan_billing_detail");

        //String sql4 = "select id,0 as nowAmount,CAST(if(amount.beforeValue='' or amount.beforeValue is null ,0,CAST (amount.beforeValue AS DECIMAL))  AS DECIMAL) as beforeAmount,borrower.nowValue as borrower from dwb_qydproduction_mt_loan where status.beforeValue in ('PREPAYMENT_FINISH','FINISH') ";
        String sql4 = "select t.row_key,t.ums_id,t.id,sum(t.nowAmount)-sum(t.beforeAmount) as amount,t.borrower from (select rowkey.nowValue as row_key,_ums_id_.nowValue as ums_id,id,CAST(if(amount.nowValue='' or amount.nowValue is null ,0,CAST (amount.nowValue AS DECIMAL)) AS DECIMAL) as nowAmount,0 as beforeAmount,borrower.nowValue as borrower from dwb_qydproduction_mt_loan where status.nowValue in ('PREPAYMENT_FINISH','FINISH')" +
                " union all  " +
                "select rowkey.nowValue as row_key,_ums_id_.nowValue as ums_id,id,0 as nowAmount,CAST(if(amount.beforeValue='' or amount.beforeValue is null ,0,CAST (amount.beforeValue AS DECIMAL))  AS DECIMAL) as beforeAmount,borrower.nowValue as borrower from dwb_qydproduction_mt_loan where status.beforeValue in ('PREPAYMENT_FINISH','FINISH')) t group by t.row_key,t.ums_id,t.id,t.borrower ";

        Table table4 = streamTableEnvironment.sqlQuery(sql4);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = streamTableEnvironment.toRetractStream(table4, Row.class);
        //tuple2DataStream.print();
        SingleOutputStreamOperator<String> map = tuple2DataStream.map(new MapFunction<Tuple2<Boolean, Row>, String>() {
            @Override
            public String map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                JSONObject result = new JSONObject();
                result.put("row_key",booleanRowTuple2.f1.getField(0));
                result.put("ums_id",booleanRowTuple2.f1.getField(1));
                result.put("loan_id",booleanRowTuple2.f1.getField(2));
                result.put("amount",booleanRowTuple2.f1.getField(3));
                result.put("user_id",booleanRowTuple2.f1.getField(4));
                return result.toJSONString();
            }
        });
       // map.print();
        map.addSink(new FlinkKafkaProducer<String>("dwd_repayment", new SimpleStringSchema(),prop)).name("dwd_repayment").setParallelism(2);
        environment.execute("Flink Table Json Engine");
    }
}
