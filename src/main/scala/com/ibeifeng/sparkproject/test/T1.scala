package com.ibeifeng.sparkproject.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description: repartition和spark.default.parallelism区别
  * spark.default.parallelism只有在处理RDD时才会起作用，对Spark SQL的无效。
  * spark.sql.shuffle.partitions则是对sparks SQL专用的设置
  * @Author: wanghailin
  * @Date: 2019/4/16
  */
class T1 {

  def f1(sc:SparkContext): Unit ={
    val rdd = sc.parallelize(1 to 100,10)
    println("[原始RDD] rdd.partitions.length="+rdd.partitions.length)//10
    val mapRdd = rdd.map(x=>(x,x))
    println("[map映射后的RDD] mapRdd.partitions.length="+mapRdd.partitions.length)//10

    val rePRdd = mapRdd.repartition(20)
    println("[repartition(20)] rePRdd.partitions.length="+rePRdd.partitions.length)//20

    val mapRdd2 = rePRdd.map(x=>x)
    println("[再map] mapRdd2.partitions.length="+mapRdd2.partitions.length)//20

    //如果conf设置了 spark.default.parallelism 这个属性,
    // 那么在groupByKey操作(这里的groupByKey指shuffle操作的算子)不指定参数时会默认到的读取设置的默认并行度参数
    val groupRdd = mapRdd2.groupByKey()
    println("[groupByKey] groupRdd.partitions.length="+groupRdd.partitions.length)//4
  }
}

object T1{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("day35").setMaster("local")
    conf.set("spark.default.parallelism","4")//设置默认的并行度
    val sc = new SparkContext(conf)
    val t = new T1
    t.f1(sc)
    sc.stop()
  }
}
