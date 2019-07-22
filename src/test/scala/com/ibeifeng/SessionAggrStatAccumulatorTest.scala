package com.ibeifeng

import com.ibeifeng.sparkproject.constant.Constants
import com.ibeifeng.sparkproject.spark.session.SessionAggrStatAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.assertTrue
import org.junit.Test

/**
  * @Description: 测试自定义
  * @Author: wanghailin
  * @Date: 2019/4/15
  */
class SessionAggrStatAccumulatorTest {

  @Test
  def AccumulatorTest() = {
    val conf = new SparkConf()
      .setAppName("SessionAggrStatAccumulatorTest")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator
    //sc.register(sessionAggrStatAccumulator,"sessionAggrStatAccumulator")

    sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
    sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
    sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
    sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)

    println(sessionAggrStatAccumulator.value)
  }

}
