package priv.zh.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import priv.zh.po.Student;

import java.sql.*;

public class MysqlSource extends RichSourceFunction<Student> {

    private static final Logger logger = LoggerFactory.getLogger(MysqlSource.class);

    private Connection con;

    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration configuration) throws Exception{
        logger.info("加载数据库驱动并且获得连接");
         String sqlStr="select * from student";
         getConnection();
         preparedStatement = con.prepareStatement(sqlStr);
         logger.info("获得数据库连接成功");
    }

    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            Student student = new Student();
            student.setAge(resultSet.getInt("age"));
            student.setName(resultSet.getString("name"));
            sourceContext.collect(student);
        }
    }

    @Override
    public void close() throws Exception{
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(con!=null){
            con.close();
        }
    }

    @Override
    public void cancel() {

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
