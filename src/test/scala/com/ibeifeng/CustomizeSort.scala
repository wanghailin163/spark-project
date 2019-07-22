package com.ibeifeng

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Description: 自定义排序
  * @Author: wanghailin
  * @Date: 2019/7/6
  */
object CustomizeSort {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("CustomizeSort")

    val sparkSession = SparkSession.builder().master("local").config(conf).getOrCreate()

    import sparkSession.implicits._

    val df = Seq(
      ("0001","245978420","贷",99.34,3700.00,"20190707","123237"),
      ("0001","12231234","借",20.00,2880.00,"20190705","123200"),
      ("0001","12231234","借",1000.00,2000.00,"20190707","123100"),
      ("0001","245978420","贷",0.01,4500.56,"20190705","122800"),
      ("0001","115103","贷",60.66,3760.66,"20190707","123237"),
      ("0001","245978420","借",200.56,4300.00,"20190705","122800"),
      ("0001","12231234","贷",200.00,3000.00,"20190706","123100"),
      ("0001","12231234","贷",500.00,2500.00,"20190707","123100"),
      ("0001","245978420","贷",1.00,3601.66,"20190707","123237"),
      ("0001","115103","贷",100.00,3700.00,"20190707","123237"),



      ("0001","115103","贷",0.34,3761.00,"20190707","123237"),
      ("0001","115103","借",60.66,2701.34,"20190707","123238"),

      ("0001","12231234","贷",200.00,2700.00,"20190707","123100"),
      ("0001","245978420","借",20.33,5000.00,"20190705","122800"),
      ("0001","245978420","贷",300.66,3600.66,"20190707","123237"),
      ("0001","245978420","借",499.45,4500.55,"20190705","122800"),
      ("0001","115103","借",1.34,2700.00,"20190707","123238"),
      ("0001","12231234","贷",20.00,3000.00,"20190705","123100"),
      ("0001","245978420","借",1000.00,3300.00,"20190707","123237"),
      ("0001","12231234","借",80.00,2800.00,"20190705","153100"),
      ("0001","115103","借",1000.00,2761.00,"20190707","123238"),
      ("0001","245978420","贷",0.30,3600.96,"20190707","000000")
    ).toDF("org_no","hqckzh","jdbz","jyje","zhye","hxjyrq","hxjysj")

    df.rdd.map(
      row=>{
        val org_no = row.getAs[String]("org_no")
        val hqckzh = row.getAs[String]("hqckzh")
        val jdbz = row.getAs[String]("jdbz")
        val jyje = row.getAs[Double]("jyje")
        val zhye = row.getAs[Double]("zhye")
        val hxjyrq = row.getAs[String]("hxjyrq")
        val hxjysj = row.getAs[String]("hxjysj")
        ((org_no,hqckzh),Jymx(org_no,hqckzh,jdbz,jyje,zhye,hxjyrq,hxjysj))
      }
    ).groupByKey()
      .foreach(kv=>{
        val iter = kv._2.toList.sortBy[Jymx](jymx=>Jymx(jymx.org_no,jymx.hqckzh,jymx.jdbz,jymx.jyje,jymx.zhye,jymx.hxjyrq,jymx.hxjysj))
        iter.foreach(println(_))
      })

      //.count()

  }

  case class Jymx(org_no: String,hqckzh: String,jdbz: String,jyje: Double,zhye: Double,hxjyrq: String,hxjysj: String)
    extends Ordered[Jymx]{
    override def compare(that: Jymx): Int = {
        if(this.hxjyrq.toInt-that.hxjyrq.toInt!=0){
          this.hxjyrq.toInt-that.hxjyrq.toInt
        }else{
          if(this.hxjysj.toInt-that.hxjysj.toInt!=0){
            this.hxjysj.toInt-that.hxjysj.toInt
          }else{
            if(this.jdbz.equals("贷")&&this.jdbz.equals(that.jdbz)){
              if(this.zhye-that.zhye>0){
                Math.ceil(this.zhye-that.zhye).toInt
              }else if(this.zhye-that.zhye<0){
                Math.floor(this.zhye-that.zhye).toInt
              }else{
                -1
              }
            }else if(this.jdbz.equals("借")&&this.jdbz.equals(that.jdbz)){
              if(that.zhye-this.zhye>0){
                Math.ceil(that.zhye-this.zhye).toInt
              }else if(that.zhye-this.zhye<0){
                Math.floor(that.zhye-this.zhye).toInt
              }else{
                -1
              }
            }else if(this.jdbz.equals("贷")&&that.jdbz.equals("借")){
              if(this.zhye-that.zhye>0){
                Math.ceil(this.zhye-that.zhye).toInt
              }else if(this.zhye-that.zhye<0){
                Math.floor(this.zhye-that.zhye).toInt
              }else{
                -1
              }
            }else{
              if(that.zhye-this.zhye>0){
                Math.ceil(that.zhye-this.zhye).toInt
              }else if(that.zhye-this.zhye<0){
                Math.floor(that.zhye-this.zhye).toInt
              }else{
                -1
              }
            }
          }
        }
    }
  }


}
