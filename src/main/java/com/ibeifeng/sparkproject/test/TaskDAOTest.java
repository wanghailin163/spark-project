package com.ibeifeng.sparkproject.test;

import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;

/**
 * @Description: 任务管理DAO测试类
 * @Author: wanghailin
 * @Date: 2019/4/10
 */
public class TaskDAOTest {

    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(2);
        System.out.println(task.getTaskName());
    }
}
