package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITop10SessionDAO;
import com.ibeifeng.sparkproject.domain.Top10Session;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * @Description: TODO
 * @Author: wanghailin
 * @Date: 2019/4/20
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values(?,?,?,?)";

        Object[] params = new Object[]{top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
