package com.ibeifeng.sparkproject.test

import java.util.UUID

import com.ibeifeng.sparkproject.util.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * @Description: 模拟数据程序
  * @Author: wanghailin
  * @Date: 2019/4/11
  */
object MockData {

  def mock(sparkSession: SparkSession):Unit={

    import sparkSession.implicits._

    var action_rows = ListBuffer[(String,Int,String,Int,String,String,Int,Int,String,String,String,String)]()

    val searchKeywords = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺",
      "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val date = DateUtils.getTodayDate()
    val actions = Array("search", "click", "order", "pay")
    val random = Random

    for(i <- 0 to 100){
      val userid: Int = random.nextInt(100)
      for(j <- 0 to 10){
        val sessionid: String = UUID.randomUUID().toString().replace("-","")
        val baseActionTime = date+" "+random.nextInt(23)
        for(k <- 0 to random.nextInt(100)){
          val pageid: Int = random.nextInt(10)
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = ""
          var clickCategoryId: Int = 0
          var clickProductId: Int = 0
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null

          val action = actions(random.nextInt(4))
          if ("search" == action) {
            searchKeyword = searchKeywords(random.nextInt(10))
          }
          else if ("click" == action) {
            clickCategoryId = random.nextInt(100)
            clickProductId = random.nextInt(100)
          }
          else if ("order" == action) {
            orderCategoryIds = random.nextInt(100)+""
            orderProductIds = random.nextInt(100)+""
          }
          else if ("pay" == action) {
            payCategoryIds = random.nextInt(100)+""
            payProductIds = random.nextInt(100)+""
          }

          action_rows+=Tuple12(date,userid,sessionid,pageid,actionTime,searchKeyword,clickCategoryId,clickProductId,
            orderCategoryIds,orderProductIds,payCategoryIds,payProductIds)

        }
      }
    }

    sparkSession.sparkContext.makeRDD(action_rows)
      .toDF("date","user_id","session_id","page_id","action_time","search_keyword","click_category_id","click_product_id",
      "order_category_ids","order_product_ids","pay_category_ids","pay_product_ids")
      .createOrReplaceTempView("user_visit_action")

    /**
      * ==================================================================
      */

    var user_rows = ListBuffer[(Int,String,String,Int,String,String,String)]()

    val sexes = Array("male", "female")
    for(i <- 0 to 100){
      var userid = i
      var username = "user"+i
      var name = "name"+i
      var age = random.nextInt(60)
      var professional = "professional" + random.nextInt(100)
      var city = "city" + random.nextInt(100)
      var sex = sexes(random.nextInt(2))

      user_rows += Tuple7(userid,username,name,age,professional,city,sex)
    }

    sparkSession.sparkContext.makeRDD(user_rows)
        .toDF("user_id","username","name","age","professional","city","sex")
        .createOrReplaceTempView("user_info")

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("mockdata")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    mock(sparkSession)
    sparkSession.sql("select * from user_visit_action").show(10)
    sparkSession.sql("select * from user_info").show(10)

    //sparkSession.close()


  }

}
