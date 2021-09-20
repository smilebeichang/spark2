package cn.edu.sysu.dao;

import cn.edu.sysu.bean.Employee;
import cn.edu.sysu.gmall_publisher.mapper.EmployeeMapper;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/6/28 16:43
 */
public class EmployeeMapperTest {

    SqlSessionFactory sqlSessionFactory;
    SqlSession session;
    EmployeeMapper mapper;
    {
        //从当前的类路径获取mybatis的配置文件
        String resource = "mybatis-config.xml";
        //使用一个流读取配置文件
        InputStream inputStream = null;
        try {
            inputStream = Resources.getResourceAsStream(resource);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //根据配置文件，创建一个SqlSessionFactory
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);

    }

    @Before
    public void init(){
        session = sqlSessionFactory.openSession(true);
        mapper = session.getMapper(EmployeeMapper.class);

    }

    @After
    public void end(){
        session.close();
    }

    @Test
    public void getEmployeeById() {
        Employee employee = mapper.getEmployeeById(2);
        System.out.println(employee);
    }

    @Test
    public void deleteEmployeeById() {
        Boolean aBoolean = mapper.deleteEmployeeById(2);
        System.out.println(aBoolean);
    }

    @Test
    public void insertEmployee() {
        Employee employee=new Employee(null, "Jackie", "male", "Jackie@qq.com");
        mapper.insertEmployee(employee);
    }

    @Test
    public void updateEmployee() {
        Employee employee=new Employee(2, "小胖", "male", "xiaopang@qq.com");
        mapper.updateEmployee(employee);
    }

    @Test
    public void getAllEmployee() {
        List<Employee> list = mapper.getAllEmployee();
        System.out.println(list);
    }
}