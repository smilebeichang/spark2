package cn.edu.sysu.dao;

import cn.edu.sysu.bean.Employee;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

/**
 * @Author : song bei chang
 * @create 2021/6/28 15:01
 */
public class dao {

    public static void main(String[] args) throws IOException {
        //从当前的类路径获取mybatis的配置文件
        String resource = "mybatis-config.xml";
        //使用一个流读取配置文件
        InputStream inputStream = Resources.getResourceAsStream(resource);
        //根据配置文件，创建一个SqlSessionFactory
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        SqlSession session = sqlSessionFactory.openSession();

        try{
            Object emp = session.selectOne("helloworld.selectEmployee", "1");
            Employee emp1 = (Employee) emp;
            System.out.println(emp1.toString());
        }finally{
            session.close();
        }
    }

}



