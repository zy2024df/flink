package com.youshu;


import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.google.gson.Gson;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Properties;


public class MysqlToPhoenixTest {


    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
        env.enableCheckpointing(10000L);
        //2.2 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 600000L));
        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("file:///D:/ckdata"));
        env.setStateBackend(new FsStateBackend("hdfs://test-cluster102:8020/flinkCDC"));
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "root");
//        System.setProperty("user.timezone","GMT+8");
//        System.setProperty("user.timezone","GMT+8");
        //2.9 创建保存配置
//        Properties properties = new Properties();
////        properties.setProperty("scan.startup.mode", "initial");
//        properties.setProperty("server-time-zone","Asia/Shanghai");
        //3创建MysqlSource
        DebeziumSourceFunction<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("114.55.73.174")
                .port(3306)
//                .debeziumProperties(properties)
                .serverTimeZone("Asia/Shanghai")
                .username("super")
                .password("hMh90XJYdy4KfxM7QR8V")
                .databaseList("novel")
                .tableList("novel.na_user","novel.na_user_mobile","novel.na_user_attention","novel.na_user_coupons","novel.na_user_exif")
                //.tableList("novel.na_user_mobile")
                .startupOptions(StartupOptions.latest())
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }

                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        HashMap<String, Object> hashMap = new HashMap<>();

                        String topic = sourceRecord.topic();
                        String[] split = topic.split("[.]");
                        String database = split[1];
                        String table = split[2];
                        hashMap.put("database", database);
                        hashMap.put("table", table);

                        //获取操作类型
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        //获取数据本身
                        Struct struct = (Struct) sourceRecord.value();
                        Struct after = struct.getStruct("after");
                        Struct before = struct.getStruct("before");
        /*
         	 1 同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
             2 只存在 beforeStruct 就是delete数据
             3 只存在 afterStruct数据 就是insert数据
        */

                        if (before != null && after != null) {
                            //update
                            Schema schema = after.schema();
                            HashMap<String, Object> hm = new HashMap<>();
                            for (Field field : schema.fields()) {
                                hm.put(field.name(), after.get(field.name()));
                            }
                            hashMap.put("data", hm);
                        }
                         else if (after != null) {
                            //insert
                            Schema schema = after.schema();
                            HashMap<String, Object> hm = new HashMap<>();
                            for (Field field : schema.fields()) {
                                hm.put(field.name(), after.get(field.name()));
                            }
                            hashMap.put("data", hm);
                        } else if (before != null) {
                            //delete
                            Schema schema = before.schema();
                            HashMap<String, Object> hm = new HashMap<>();
                            for (Field field : schema.fields()) {
                                hm.put(field.name(), before.get(field.name()));
                            }
                            hashMap.put("data", hm);
                        }

//                        String type = operation.toString().toLowerCase();
//                        if ("create".equals(type)) {
//                            type = "insert";
//                        } else if ("delete".equals(type)) {
//                            type = "delete";
//                        } else if ("update".equals(type)) {
//                            type = "update";
//                        }
//                        hashMap.put("type", type);

                         hashMap.put("type",operation.toString().toLowerCase());

                        Gson gson = new Gson();
                        collector.collect(gson.toJson(hashMap));

                    }
                })

//                .deserializer(new DebeziumDeserializationSchema<String>() {  //自定义数据解析器
//                    @Override
//                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//                        //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink.z_user_info
//                        String topic = sourceRecord.topic();
//                        String[] arr = topic.split("\\.");
//                        String db = arr[1];
//                        String tableName = arr[2];
//                        //获取操作类型 READ DELETE UPDATE CREATE
//                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//                        //获取值信息并转换为Struct类型
//                        Struct value = (Struct) sourceRecord.value();
//                        //获取变化后的数据
//                        Struct after = value.getStruct("after");
//                        //创建JSON对象用于存储数据信息
//                        JSONObject data = new JSONObject();
//                        for (Field field : after.schema().fields()) {
//                            Object o = after.get(field);
//                            data.put(field.name(), o);
//                        }
//                        //创建JSON对象用于封装最终返回值数据信息
//                        JSONObject result = new JSONObject();
//                        result.put("type", operation.toString().toLowerCase());
////                        String type = operation.toString().toLowerCase();
////                        if ("create".equals(type)) {
////                            type = "insert";
////                        } else if ("delete".equals(type)) {
////                            type = "delete";
////                        } else if ("update".equals(type)) {
////                            type = "update";
////                        }
////                        result.put("type",type);
//                        result.put("data", data);
//                        result.put("database", db);
//                        result.put("table", tableName);
//                        //发送数据至下游
//                        collector.collect(result.toJSONString());
//                    }
//                    @Override
//                    public TypeInformation<String> getProducedType() {
//                        return TypeInformation.of(String.class);
//                    }
//                })
                .build();

        //4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        //5.打印数据
        mysqlDS.print();

//        mysqlDS.addSink(new PhoenixSinkTest());

          mysqlDS.addSink(new PhoenixSinkTest01());

//        env.addSource(mysqlSource).addSink(new PhoenixSinkTest());


        //6.执行任务
        env.execute();

    }

}
