package com.ibeifeng.sparkproject.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * @Description: TODO
 * @Author: wanghailin
 * @Date: 2019/11/29
 */
public class PropertiesUtil implements Serializable {

    private static Logger log = LoggerFactory.getLogger(PropertiesUtil.class);
    public String fileName;
    Properties pps = new Properties();

    public PropertiesUtil(String fileName){
        this.fileName=fileName;
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = this.getClass().getResourceAsStream(fileName);
            pps.load(resourceAsStream);
        }catch (IOException e){
            log.error("加载文件异常，文件是{}",fileName,e);
        }finally {
            if(resourceAsStream!=null){
                try {
                    resourceAsStream.close();
                }catch (IOException e){
                    log.error("关闭resourceAsStream异常",e);
                }
            }
        }
    }

    public String getString(String key){
        return pps.getProperty(key);
    }

    public int getInt(String key){
        return Integer.parseInt(pps.getProperty(key));
    }

    public boolean getBoolean(String key){
        return Boolean.parseBoolean(pps.getProperty(key));
    }

}
