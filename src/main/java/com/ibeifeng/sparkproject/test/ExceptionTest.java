package com.ibeifeng.sparkproject.test;

/**
 * @Description: TODO
 * @Author: wanghailin
 * @Date: 2019/9/6
 */
public class ExceptionTest {

    public static void main(String[] args) {
        for(int i=1;i<=10;i++){
            try{
                if(i==5){
                    throw new Exception("异常");
                }
            }catch (Exception e){
                try {
                    throw new Exception("异常");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
            System.out.println(i);
        }
    }
}
