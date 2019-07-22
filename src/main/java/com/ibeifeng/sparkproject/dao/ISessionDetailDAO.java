package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.SessionDetail;

/**
 * @Description: Session明细DAO接口
 * @Author: wanghailin
 * @Date: 2019/4/17
 */
public interface ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);

}
