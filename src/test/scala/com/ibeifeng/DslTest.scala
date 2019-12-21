package com.ibeifeng

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * @Description: TODO
  * @Author: wanghailin
  * @Date: 2019/7/30
  */
object DslTest {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val deptRDD = sparkSession.sparkContext.parallelize(Seq(
      Row(10,"ACCOUNTING","NEW YORK"),
      Row(20,"RESEARCH","DALLAS"),
      Row(30,"SALES","CHICAGO"),
      Row(40,"OPERATIONS","BOSTON")
    ))

    val empRDD = sparkSession.sparkContext.parallelize(Seq(
      Row(7369,"SMITH","职员",7566,java.sql.Date.valueOf("1980-12-17"),800.00,null,20),
      Row(7499,"ALLEN","销售员",7698,java.sql.Date.valueOf("1981-02-20"),1600.00,300.00,30),
      Row(7521,"WARD","销售员",7698,java.sql.Date.valueOf("1981-02-22"),1250.00,500.00,30),
      Row(7566,"JONES","经理",7839,java.sql.Date.valueOf("1981-04-02"),2975.00,null,20),
      Row(7654,"MARTIN","销售员",7698,java.sql.Date.valueOf("1981-09-28"),1250.00,1400.00,30),
      Row(7698,"BLAKE","经理",7839,java.sql.Date.valueOf("1981-05-01"),2850.00,null,30),
      Row(7782,"CLARK","经理",7839,java.sql.Date.valueOf("1981-06-09"),2450.00,null,10),
      Row(7788,"SCOTT","职员",7566,java.sql.Date.valueOf("1987-07-03"),3000.00,2000.00,20),
      Row(7839,"KING","董事长",null,java.sql.Date.valueOf("1981-11-17"),5000.00,null,10),
      Row(7844,"TURNERS","销售员",7698,java.sql.Date.valueOf("1981-09-08"),1500.00,50.00,30),
      Row(7876,"ADAMS","职员",7566,java.sql.Date.valueOf("1987-07-13"),1100.00,null,20),
      Row(7900,"JAMES","职员",7698,java.sql.Date.valueOf("1981-12-03"),1250.00,null,30),
      Row(7902,"FORD","销售员",7566,java.sql.Date.valueOf("1981-12-03"),3000.00,null,20),
      Row(7934,"MILLER","职员",7782,java.sql.Date.valueOf("1981-01-23"),1300.00,null,10)
    ))

    val salRDD = sparkSession.sparkContext.parallelize(Seq(
      Row(1,500.00,1000.00),
      Row(2,1001.00,1500.00),
      Row(3,1501.00,2000.00),
      Row(4,2001.00,3000.00),
      Row(5,3001.00,9999.00)
    ))

    val deptschema = StructType(List(
      StructField("deptno", IntegerType, nullable = false),
      StructField("dname", StringType, nullable = true),
      StructField("loc", StringType, nullable = true)
    ))
    val empschema = StructType(List(
      StructField("empno", IntegerType, nullable = false),
      StructField("ename", StringType, nullable = true),
      StructField("job", StringType, nullable = true),
      StructField("mgr", IntegerType, nullable = true),
      StructField("hiredate", DateType, nullable = true),
      StructField("sal", DoubleType, nullable = true),
      StructField("comm", DoubleType, nullable = true),
      StructField("deptno", IntegerType, nullable = true)
    ))
    val salschema = StructType(List(
      StructField("grade", IntegerType, nullable = false),
      StructField("losal", DoubleType, nullable = true),
      StructField("hisal", DoubleType, nullable = true)
    ))

    val deptDF = sparkSession.sqlContext.createDataFrame(deptRDD,deptschema)
    val empDF = sparkSession.sqlContext.createDataFrame(empRDD,empschema)
    val salDF = sparkSession.sqlContext.createDataFrame(salRDD,salschema)

    /*empDF.alias("emp").filter("emp.deptno=30").show()
    empDF.alias("emp").filter("emp.job='职员'").select("emp.empno","emp.ename","emp.deptno").show()
    empDF.alias("emp").filter("emp.comm>emp.sal").show()
    empDF.alias("emp").filter("emp.comm>sal*0.6").show()
    empDF.alias("emp").filter("emp.ename like '%A%'").show()
    empDF.alias("emp").filter("emp.ename like 'A%' or emp.ename like 'B%' or emp.ename like 'S%'").show()
    empDF.alias("emp").filter("length(ename)=7").show()
    empDF.alias("emp").filter("emp.ename not like '%r%'").show()*/


    //排序中orderBy和sort的用法和结果是相同的
    /*import org.apache.spark.sql.functions._
    empDF.alias("emp").orderBy(desc("emp.ename")).orderBy(asc("emp.sal")).select("ename","sal").show()
    //cast函数表示将结果转换成另外一种数据类型  decimal(10,2)表示 最大10位数字其中包括小数点后两位
    empDF.alias("emp").selectExpr("emp.ename as name","cast(emp.sal/30 as decimal(10,2)) as 30avgsal").show()
    //将comm为null的置为0并且将当前结果转换为decimal(10,0)类型，再将该结果转为string类型
    empDF.alias("emp").selectExpr("emp.ename name","cast(cast(nvl(comm,0) as decimal(10,0)) as string) comm").show()

    //聚合每个部门的人数
    empDF.alias("emp").groupBy("emp.deptno").agg(count("emp.empno").alias("cno"))
      .selectExpr("emp.deptno","case when cno>=5 then '人数较多' else '人数较少' end as desc").show()
    //聚合每个部门的所有员工姓名
    empDF.alias("emp").groupBy("emp.deptno").agg(collect_list("emp.ename").alias("names"))
      .selectExpr("deptno","names").show()
    //聚合每个部门 并且 统计每个部门的职位数量
    empDF.alias("emp").groupBy("emp.deptno").agg(countDistinct("emp.job").alias("cono"))
      .selectExpr("deptno","cono").show()*/


    /*import sparkSession.implicits._
    //left semi join是以左表为准，在右表中查找匹配的记录，如果查找成功，则仅返回左边的记录，否则返回null，
    empDF.alias("emp").join(deptDF.as("dept"),($"emp.deptno"===$"dept.deptno"),"leftsemi").show()
    //left anti join与left semi join相反，是以左表为准，在右表中查找匹配的记录，如果查找成功，则返回null，否则仅返回左边的记录
    empDF.alias("emp").join(deptDF.as("dept"),($"emp.deptno"===$"dept.deptno"),"leftanti").show()*/


    /*import org.apache.spark.sql.functions._
    empDF.alias("emp").join(broadcast(deptDF).as("dept"),col("emp.deptno")===col("dept.deptno"),"left")
      .withColumn("desc",when(col("emp.comm").isNotNull/*.and(col("emp.comm").notEqual(""))*/,"有奖金").otherwise("没有奖金"))
      .filter("emp.job='职员'")
      .selectExpr("emp.empno","emp.ename","emp.job","emp.sal","emp.deptno","dept.dname","desc").alias("tmp")
      .join(salDF.alias("sal"),(col("tmp.sal")>col("sal.losal")).and(col("tmp.sal")<col("sal.hisal")))
      .filter("tmp.deptno=10")
      .selectExpr("tmp.empno","tmp.ename","tmp.sal","sal.grade","tmp.deptno","tmp.dname","desc").show()*/

    //empDF.show()
    //deptDF.show()
    //salDF.show()

    //import sparkSession.implicits._
    //empDF.alias("emp").join(deptDF.alias("dept"),$"emp.deptno"==="dept.deptno","inner").show()

    import sparkSession.implicits._
    empDF.alias("emp").join(deptDF.as("dept"),($"emp.deptno"===$"dept.deptno"),"leftanti").show()

  }

}
