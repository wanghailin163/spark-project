package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ISessionRandomExtractDAO;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * @Description: 随机抽取session的DAO实现
 * @Author: wanghailin
 * @Date: 2019/4/17
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

    /**
     * 插入session随机抽取
     * @param sessionRandomExtract
     */
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";

        Object[] params = new Object[]{sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
