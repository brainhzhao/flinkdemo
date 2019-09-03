package priv.zh;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaConsumerDemo {

    public static void main(String[] args) throws Exception{
        //首先获取执行环境
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置kafkaconsumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.255.129:9092");
        properties.put("zookeeper.connect","192.168.255.129:2181");
        properties.put("group.id","test-01-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value 反序列化
        properties.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = environment.addSource(
                new FlinkKafkaConsumer011<>("test",new SimpleStringSchema(),properties)
        ).setParallelism(1);
        dataStreamSource.print();
        environment.execute("read kafka data");
    }
}