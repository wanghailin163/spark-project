package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * @Description: TODO
 * @Author: wanghailin
 * @Date: 2019/4/20
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    @Override
    public void insert(Top10Category category) {

        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);

    }
}
