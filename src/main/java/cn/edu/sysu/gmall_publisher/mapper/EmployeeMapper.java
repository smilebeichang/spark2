package cn.edu.sysu.gmall_publisher.mapper;

import cn.edu.sysu.bean.Employee;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/6/28 16:31
 */
public interface EmployeeMapper {

    /**
     * 查询
     */
    Employee getEmployeeById(int id);

    /**
     * 删除
     */
    Boolean deleteEmployeeById(int id);

    /**
     * 插入
     */
    void insertEmployee(Employee employee);

    /**
     * 更新
     */
    void updateEmployee(Employee employee);

    /**
     * 查询
     */
    List<Employee> getAllEmployee();

}
