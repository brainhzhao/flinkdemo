package priv.zh;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TimeUtils;
import priv.zh.po.Student;
import priv.zh.sink.MysqlSink;

import javax.annotation.Nullable;
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

        FlinkKafkaConsumer011 kafkaConsumer =new FlinkKafkaConsumer011<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                properties);
        kafkaConsumer.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Object o, long l) {
                return null;
            }

            @Override
            public long extractTimestamp(Object o, long l) {
                return 0;
            }
        });
        //set source
        DataStream<Student> dataStream= env.addSource(kafkaConsumer)
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
       /*dataStream = dataStream.filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student student) throws Exception {
                if(student.getSex().equals("男")){
                    return false;
                }
                return true;
            }
        });*/
        //handle keyBy
        KeyedStream<Student,Integer> keyedStream = dataStream.keyBy(new KeySelector<Student,Integer>(){
            @Override
            public Integer getKey(Student student){
                return student.getAge();
            }
        });

        //handle reduce
     /*   keyedStream.reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student student, Student t1) throws Exception {
                Student returnStuent = new Student();
                returnStuent.setAge(student.getAge()+t1.getAge());
                returnStuent.setName(student.getName()+"_link_"+t1.getName());
                returnStuent.setSex(student.getSex());
                returnStuent.setCountry(student.getCountry());
                return returnStuent;
            }
        })*/
/*        DataStream<String> foldStream=keyedStream.fold("fold", new FoldFunction<Student, String>() {
            @Override
            public String fold(String r, Student o) throws Exception {
                return r+o.getName();
            }
        });*/

        WindowedStream windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //foldStream.print();
        windowedStream.reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student o, Student t1) throws Exception {
                Student student = new Student();
                student.setName(o.getName()+t1.getName());
                student.setSex(o.getSex()+t1.getSex());
                return student;
            }
        }).print();

                //.map((student -> JSON.toJSONString(student))).print();
        env.execute("kafka to mysql_handle_map");
    }
}