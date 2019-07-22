package com.ibeifeng

import java.util

import com.ibeifeng.sparkproject.test.MockData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @Description: TODO
  * @Author: wanghailin
  * @Date: 2019/7/4
  */
object SparkMapJoin {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("SparkMapJoin")
      .master("local").getOrCreate()

    val sc = sparkSession.sparkContext

    MockData.mock(sparkSession)

    val useractionDF = sparkSession.sql("select * from user_visit_action")
    val userDF = sparkSession.sql("select * from user_info")
    val userMap = userDF.rdd.map(row=>(row.getAs[Int]("user_id"),
      (row.getAs[String]("username"),row.getAs[String]("name"),row.getAs[Int]("age"),row.getAs[String]("professional"))
    ))

    val userMapBroadCast = sc.broadcast(userMap.collectAsMap())

    import sparkSession.implicits._

    val res: RDD[(Int, String, Int, String, String)] = useractionDF.rdd.mapPartitions(actions=>{
      val usermap: collection.Map[Int, (String, String, Int, String)] = userMapBroadCast.value
      var list = ListBuffer[(Int,String,Int,String,String)]()
      var tuple = null
      actions.foreach(action=>{
        val user_id = action.getAs[Int]("user_id")
        list+=Tuple5(user_id,
          usermap.get(user_id).get._1,
          usermap.get(user_id).get._3,
          action.getAs[String]("date")
          ,action.getAs[String]("action_time"))
      })
      list.iterator
    })

    res.toDF("userid","username","age","date","action_time").createOrReplaceTempView("mapjoinresult")

    sparkSession.sql("select * from mapjoinresult").show(10)



    sparkSession.close()

  }

}
