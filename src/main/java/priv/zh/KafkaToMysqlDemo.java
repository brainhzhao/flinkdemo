package priv.zh;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import priv.zh.po.Student;
import priv.zh.sink.MysqlSink;

import java.util.Properties;

public class KafkaToMysqlDemo {
    public static void main(String[] args) throws Exception{
        //get stream evn
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置kafkaconsumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("group.id","test-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value 反序列化
        properties.put("auto.offset.reset", "latest");

        //set source
        DataStream<Student> dataStream= env.addSource(new FlinkKafkaConsumer011<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                properties))
                .setParallelism(1)
                .map((string)->JSON.parseObject(string, Student.class));
        //handle map ,add name _map
       /* dataStream=dataStream.map((student)->{
            Student newStudent = new Student();
            newStudent.setCountry(student.getCountry());
            newStudent.setName(student.getName()+"_map");
            newStudent.setAge(student.getAge()+20);
            newStudent.setSex(student.getSex());
            return newStudent;
        });*/
        //handle flatMap
       /* dataStream = dataStream.flatMap(new FlatMapFunction<Student,Student>(){
            @Override
            public void flatMap(Student student, Collector<Student> collector){
                if(student.getAge()>50){
                    collector.collect(student);
                }
            }
        });*/
       //handle filter
//        dataStream = dataStream.filter(new FilterFunction<Student>() {
//            @Override
//            public boolean filter(Student student) throws Exception {
//                if(student.getSex().equals("男")){
//                    return false;
//                }
//                return true;
//            }
//        });
        //handle keyBy
        KeyedStream<Student,Integer> keyedStream = dataStream.keyBy(new KeySelector<Student,Integer>(){
            @Override
            public Integer getKey(Student student){
                return student.getAge();
            }
        });

        keyedStream.map((student -> JSON.toJSONString(student))).print();
        env.execute("kafka to mysql_handle_map");
    }
}