package priv.zh;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import priv.zh.source.MysqlSource;

public class MysqlSourceDemo {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MysqlSource()).print();

        env.execute("mysql source task");
    }
}