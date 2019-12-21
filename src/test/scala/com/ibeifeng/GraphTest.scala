package com.ibeifeng

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @Description: TODO
  * @Author: wanghailin
  * @Date: 2019/6/20
  */
object GraphTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GraphTest").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    mockData(sparkSession)
    sparkSession.close()

  }

  case class Guar(amount: Double)

  def mockData(sparkSession: SparkSession):Unit = {

    import sparkSession.implicits._

    val guarantDF = sparkSession.sparkContext.makeRDD(
      List(
        (1000001,"一组"),(1000002,"一组"),(1000003,"二组"),
        (1000004,"二组"),(1000005,"二组"),(1000007,"二组"),
        (1000008,"三组"),(1000009,"四组"),(1000010,"五组"),
        (1000011,"五组")
      )
    )

    val guarant = sparkSession.sparkContext.makeRDD(
      List(
        (1000001,"0001公司",1000002,"0002公司",100.00),
        (1000002,"0002公司",1000003,"0003公司",200.00),
        (1000003,"0003公司",1000007,"0007公司",150.00),
        (1000007,"0007公司",1000003,"0003公司",181.00),
        (1000007,"0007公司",1000004,"0004公司",180.00),
        (1000004,"0004公司",1000005,"0005公司",300.00),
        (1000005,"0005公司",1000001,"0001公司",120.00),
        (1000009,"0009公司",1000008,"0008公司",700.00),
        (1000009,"0009公司",1000010,"0010公司",700.00),
        (1000008,"0008公司",1000011,"0011公司",666.00),
        (1000007,"0007公司",1000010,"0010公司",555.00),
        (1000006,"0006公司",1000012,"0012公司",199.00),
        (1000012,"0012公司",1000006,"0006公司",188.00),
        (1000012,"0012公司",1000006,"0006公司",200.00),
        (1000005,"0005公司",1000007,"0007公司",12000.00)
      )
    ).toDF("src_code","src_name","dst_code","dst_name","amount")
      .rdd
      .map(row=>Edge(row.getAs[Int]("src_code"),row.getAs[Int]("dst_code"),Guar(row.getAs[Double]("amount"))))

    val graph = Graph.fromEdges(guarant,Int.MaxValue)

    graph.stronglyConnectedComponents(Int.MaxValue).vertices.foreach(println(_))       //强连通
    //graph.connectedComponents(Int.MaxValue).vertices.foreach(println(_))               //弱连通

    val cycledf =
    graph.stronglyConnectedComponents(Int.MaxValue).triplets
      .map(v=>(v.srcId,v.srcAttr,v.dstId,v.dstAttr,v.attr))
      .filter(v=>(v._2==v._4))
      .map(v=>(v._2,(v._1,v._5,v._3)))
      .groupByKey()
      .flatMap(tuple=>{
        val crcle_id = tuple._1
        val edges = tuple._2
        val list = ListBuffer[(Long,Long,Double,Long)]()
        for(edge<-edges){
          list+=Tuple4(crcle_id,edge._1,edge._2.amount,edge._3)
        }
        list
      })
      .toDF("cycle_id","src_id","amount","dst_id")

    cycledf.createOrReplaceTempView("cycle")
    sparkSession.sql("select * from cycle").show()

  }

}
