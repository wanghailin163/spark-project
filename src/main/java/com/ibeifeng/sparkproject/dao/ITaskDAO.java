package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.Task;

/**
 * @Description: 任务管理DAO接口
 * @Author: wanghailin
 * @Date: 2019/4/10
 */
public interface ITaskDAO {

    /**
     * 根据主键查询任务
     * @param taskid 主键
     * @return 任务
     */
    Task findById(long taskid);
}
