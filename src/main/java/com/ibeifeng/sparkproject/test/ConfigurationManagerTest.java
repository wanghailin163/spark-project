package com.ibeifeng.sparkproject.test;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;

/**
 * @Description: 配置管理组件测试类
 * @Author: wanghailin
 * @Date: 2019/4/10
 */
public class ConfigurationManagerTest {

    public static void main(String[] args) {
        String testkey1 = ConfigurationManager.getProperty("testkey1");
        String testkey2 = ConfigurationManager.getProperty("testkey2");
        System.out.println(testkey1);
        System.out.println(testkey2);
    }
}
