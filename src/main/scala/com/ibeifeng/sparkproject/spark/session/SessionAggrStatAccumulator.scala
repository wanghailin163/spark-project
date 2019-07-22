package com.ibeifeng.sparkproject.spark.session

import com.ibeifeng.sparkproject.constant.Constants
import com.ibeifeng.sparkproject.util.StringUtils
import org.apache.spark.util.AccumulatorV2

/**
  * @Description: 自定义的session累加器
  * @Author: wanghailin
  * @Date: 2019/4/15
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String,String]{

  //统计sesion信息
  private var statisticsSession = Constants.SESSION_COUNT + "=0|" +
                                  Constants.TIME_PERIOD_1s_3s + "=0|" +
                                  Constants.TIME_PERIOD_4s_6s + "=0|" +
                                  Constants.TIME_PERIOD_7s_9s + "=0|" +
                                  Constants.TIME_PERIOD_10s_30s + "=0|" +
                                  Constants.TIME_PERIOD_30s_60s + "=0|" +
                                  Constants.TIME_PERIOD_1m_3m + "=0|" +
                                  Constants.TIME_PERIOD_3m_10m + "=0|" +
                                  Constants.TIME_PERIOD_10m_30m + "=0|" +
                                  Constants.TIME_PERIOD_30m + "=0|" +
                                  Constants.STEP_PERIOD_1_3 + "=0|" +
                                  Constants.STEP_PERIOD_4_6 + "=0|" +
                                  Constants.STEP_PERIOD_7_9 + "=0|" +
                                  Constants.STEP_PERIOD_10_30 + "=0|" +
                                  Constants.STEP_PERIOD_30_60 + "=0|" +
                                  Constants.STEP_PERIOD_60 + "=0"

  override def isZero: Boolean = {
    true
    //statisticsSession.isEmpty
  }

  override def copy(): AccumulatorV2[String, String] = {
    new SessionAggrStatAccumulator
  }

  override def reset(): Unit = {

  }

  override def add(v: String): Unit = {
    //从现有的连接串中提取v对应的值
    val oldValue = StringUtils.getFieldFromConcatString(statisticsSession,"\\|",v)
    //累计1
    val newValue = Integer.valueOf(oldValue)+1
    //给连接串中的v2设置新的累加后的值
    statisticsSession = StringUtils.setFieldInConcatString(statisticsSession,"\\|",v,String.valueOf(newValue))
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {

  }

  override def value: String = {
    statisticsSession
  }
}
