package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * @Description: TODO
 * @Author: wanghailin
 * @Date: 2019/4/17
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {
    @Override
    public void insert(SessionDetail sessionDetail) {

        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{sessionDetail.getTaskId(),
                sessionDetail.getUserId(),
                sessionDetail.getSessionId(),
                sessionDetail.getPageId(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);

    }
}
