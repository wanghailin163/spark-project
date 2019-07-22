package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.impl.*;

/**
 * @Description: DAO工厂类
 * @Author: wanghailin
 * @Date: 2019/4/10
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     * @return DAO
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

    /**
     * 获取session聚合DAO
     * @return DAO
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new SessionAggrStatDAOImpl();
    }

    /**
     * 获取session随机抽取DAO
     * @return DAO
     */
    public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
        return new SessionRandomExtractDAOImpl();
    }

    /**
     * Session明细DAO接口
     * @return DAO
     */
    public static ISessionDetailDAO getSessionDetailDAO(){
        return new SessionDetailDAOImpl();
    }

    /**
     * 获取访问前10的品类
     * @return
     */
    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAOImpl();
    }

    /**
     * 每个session访问前10品类
     * @return
     */
    public static ITop10SessionDAO getTop10SessionDAO() {
        return new Top10SessionDAOImpl();
    }

}
