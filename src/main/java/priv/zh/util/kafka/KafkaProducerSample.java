package priv.zh.util.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import priv.zh.po.Student;

import java.util.Properties;

public class KafkaProducerSample {

    public static void writeToKafka(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        Student record = new Student();
        record.setAge((int)Math.floor(Math.random()*10)/2);
        record.setName("zhaoheng"+(int)Math.floor(Math.random()*100));
        record.setSex(Math.random()<=0.5?"男":"女");
        record.setCountry(Math.random()<=0.5?"china":"us");



        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("student",null,null, JSON.toJSONString(record));

        producer.send(producerRecord);
        System.out.println("send record :"+JSON.toJSONString(record));
    }

    public static void main(String[] args) throws Exception {
        while (true){
            Thread.sleep(1000);
            writeToKafka();
        }
    }
}