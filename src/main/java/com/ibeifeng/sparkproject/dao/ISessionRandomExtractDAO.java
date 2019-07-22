package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.SessionRandomExtract;

/**
 * @Description: session随机抽取模块DAO接口
 * @Author: wanghailin
 * @Date: 2019/4/17
 */
public interface ISessionRandomExtractDAO {

    /**
     * 插入session随机抽取
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);

}
