package priv.zh.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import priv.zh.po.Student;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlSink extends RichSinkFunction<Student> {

    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);

    private Connection con;

    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception{
        logger.info("加载数据库驱动并且获得连接");
        String sqlStr="INSERT INTO student(name,PASSWORD,age) value(?,?,?);";
        getConnection();
        preparedStatement = con.prepareStatement(sqlStr);
        logger.info("获得数据库连接成功");
    }

    @Override
    public void invoke(Student value, SinkFunction.Context context) throws Exception {
        logger.info("begin sink data");
        preparedStatement.setString(1,value.getName());
        preparedStatement.setString(2,value.getCountry());
        preparedStatement.setInt(3,value.getAge());
        logger.info("update to mysql data {}",preparedStatement.executeUpdate());
    }

    @Override
    public void close()throws Exception{
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(con!=null){
            con.close();
        }
    }

    public void getConnection(){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://192.168.255.1:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root");
        }catch (ClassNotFoundException ex){
            logger.error(ex.getMessage(),ex);
        }catch (SQLException ex){
            logger.error("获取连接失败",ex);
        }
    }
}
