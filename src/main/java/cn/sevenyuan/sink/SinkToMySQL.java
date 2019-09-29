package cn.sevenyuan.sink;

import cn.sevenyuan.domain.Student;
import cn.sevenyuan.util.MyDruidUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * 将数据下沉到 MySQL 数据库中
 * @author JingQ at 2019-09-30
 */
public class SinkToMySQL extends RichSinkFunction<List<Student>> {

    private PreparedStatement ps;

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取链接
        connection = MyDruidUtils.getConnection();
        String sql = "insert into student(name, age, address) values (?, ?, ?);";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        // 关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(List<Student> value, Context context) throws Exception {
        // 遍历数据集合
        for (Student student : value) {
            ps.setString(1, student.getName());
            ps.setInt(2, student.getAge());
            ps.setString(3, student.getAddress());
            ps.addBatch();
        }
        // 一次性插入时间窗口聚合起来的数据
        int[] count = ps.executeBatch();
        System.out.println("success insert number : " + count.length);
    }
}
