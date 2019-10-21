package net.kaiyuan.kyflink.rdw;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import net.kaiyuan.kyflink.utils.Configs;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.alibaba.fastjson.JSON.parseObject;

/**
 * @author 170379
 */
public class kafkaTableSqlit {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = Configs.getProperties();
        String bootstrap = properties.getProperty("bootstrap.servers");
        String zookeeper = properties.getProperty("zookeeper.connect");
        String topic = "ums_db_qydproduction";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", bootstrap);
        prop.setProperty("group.id", "chaixiaoxue");
        prop.setProperty("zookeeper.connect", zookeeper);
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
        myConsumer.setStartFromGroupOffsets();//默认消费策略
        /*String s = HttpClientUtil.doGet("http://172.16.82.89:8080/test/flink/getConf");
        System.out.println(s);*/

        DataStreamSource<String> text = env.addSource(myConsumer);
        //text.print();
        SingleOutputStreamOperator<List<String>> map = text.map(new MapFunction<String, List<String>>() {
            @Override
            public List<String> map(String value) {
                try {
                    JSONObject umsObject = parseObject(value);
                    JSONObject schema = umsObject.getJSONObject("schema");
                    Boolean isDdl = schema.getBoolean("ddl");
                    List<String> reuslt = new ArrayList<>();
                    JSONObject changeUms = new JSONObject();
                    JSONArray fields = schema.getJSONArray("fields");
                    JSONArray beforeUpdated = umsObject.getJSONArray("beforeUpdated");
                    JSONObject header = umsObject.getJSONObject("header");
                    JSONArray payload = umsObject.getJSONArray("payload");
                    //Map<String,Object> map = new HashMap<>();
                    changeUms.put("dbName", header.getString("schemaName"));
                    changeUms.put("tableName", header.getString("tableName"));
                    changeUms.put("ddl", isDdl);
                    if (isDdl){
                        changeUms.put("ddlSql", schema.getString("ddlSql"));
                        reuslt.add(changeUms.toJSONString());
                        return reuslt;
                    }else {
                        changeUms.put("ddlSql", schema.getString(""));
                        for (int i = 0; i < payload.size(); i++) {
                            Map<String, Object> fieldsMap = new HashMap<>();
                            JSONArray tuple = payload.getJSONObject(i).getJSONArray("tuple");
                            JSONObject beforeUpdated_sub=null;
                            if(tuple.get(2).equals("u")){
                                beforeUpdated_sub = beforeUpdated.getJSONObject(i);
                            }
                            for (int j = 0; j < fields.size(); j++) {
                                JSONObject jb = fields.getJSONObject(j);
                                String name = jb.getString("name");
                                if (name.equals("id")) {
                                    changeUms.put("id", tuple.getString(j));
                                }
                                Map<String, Object> fliedMap = new HashMap();
                                fliedMap.put("nowValue", tuple.getString(j));
                                fliedMap.put("type", jb.getString("type"));
                                fliedMap.put("updated", jb.getBooleanValue("updated"));
                                if (beforeUpdated_sub != null && beforeUpdated_sub.containsKey(name)) {
                                    fliedMap.put("beforeValue", beforeUpdated_sub.getString(name));
                                } else {
                                    fliedMap.put("beforeValue","");
                                }
                                fieldsMap.put(name, fliedMap);
                            }
                            changeUms.put("fields", fieldsMap);
                            reuslt.add(changeUms.toJSONString());
                            return reuslt;
                        }
                    }
                }catch (Exception e){
                    System.out.println("转换格式出错"+e);
                }
                return null;
            }
        });
        //map.print();
        map.flatMap(new FlatMapFunction<List<String>, String>() {

            @Override
            public void flatMap(List<String> value, Collector<String> out) {
                for (String s : value){
                    //System.out.println(s);
                    out.collect(s);
                }
            }
        }).addSink(new FlinkKafkaProducer<String>("dwb_default_topic", new KeyedSerializationSchema<String>() {

            @Override
            public byte[] serializeKey(String element) {
                JSONObject line = parseObject(element);
                if (line.containsKey("id")){
                    try {
                        return line.getString("id").getBytes("UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                return  new byte[0];
            }

            @Override
            public byte[] serializeValue(String element) {
                try {
                    return element.getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
               return element.getBytes();
            }

            @Override
            public String getTargetTopic(String element) {
                JSONObject line = parseObject(element);
                if (line.getBoolean("ddl")){
                    return "dwb_ddl_topic";
                }
                if (line.containsKey("tableName")){
                    //TODO:读取服务配置 然后将需要的添加到topic里面,或者读取redis
                    String tableName = line.getString("tableName");
                    if (tableName.equals("mt_loan_billing") || tableName.equals("mt_loan_billing_detail") || tableName.equals("mt_loan")){
                        System.out.println(tableName);
                        return "dwb_"+line.getString("databaseName")+"_"+line.getString("tableName");
                    }
                }
                return "dwb_default_topic";
            }
        }, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("test-kafka").setParallelism(5);
        env.execute("StreamingFromCollection");

    }
}
