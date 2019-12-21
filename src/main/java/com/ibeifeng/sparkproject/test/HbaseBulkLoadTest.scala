package com.ibeifeng.sparkproject.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


/**
  * @Description: TODO
  * @Author: wanghailin
  * @Date: 2019/11/29
  */
object HbaseBulkLoadTest {


  val hdfsRootPath = "hdfs://tmp/"
  val hFilePath = "hdfs://tmp/test/whl/hbase/bulkload/hfile/"
  val tableName="whltest"
  val familyName = "cf1"

  def main(args: Array[String]): Unit = {


    /***********************************初始化信息***********************************/
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()//sparkSession
    //hdfs配置信息
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", hdfsRootPath)
    val fileSystem = FileSystem.get(hadoopConf)
    //创建hbase配置信息
    val hbconf = HBaseConfiguration.create(hadoopConf)
    //zookeeper机器所在地址
    hbconf.set("hbase.zookeeper.quorum","cdh2.iwellmass.com,cdh4.iwellmass.com,scm.iwellmass.com")
    //zookeeper端口
    hbconf.set("hbase.zookeeper.property.clientPort","2181")
    //hbase表名称
    hbconf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    //获取hbase连接对象
    val hbaseConn = ConnectionFactory.createConnection(hbconf)
    val admin = hbaseConn.getAdmin
    /********************************************************************************/


    // 如果 HBase 表不存在，就创建一个新表
    /*if (!admin.tableExists(TableName.valueOf(tableName))) {
      val desc = new HTableDescriptor(TableName.valueOf(tableName))//初始化表
      val hcd = new HColumnDescriptor(familyName)//初始化列族
      hcd.setCompressionType(Compression.Algorithm.SNAPPY)//列族设置压缩方式
      desc.addFamily(hcd)//添加列族
      admin.createTable(desc)//创建hbase表
    }
    // 如果存放 HFile文件的路径已经存在，就删除掉
    if(fileSystem.exists(new Path(hFilePath))) {
      fileSystem.delete(new Path(hFilePath), true)
    }*/

    println("**************************汇总表的指标**************************")
    val validCust: DataFrame = spark.sql(
      """
        |select id,unified_code,org_no,zhmc,hqckzh,yxjgmc from graph.z_gl_account_wid
      """.stripMargin
    ).cache()

    val columnsName: Array[String] = validCust.columns.sorted  //获取列名 第一个为key

    val result =
    validCust.repartition(5).rdd.map(row=>{
      var kvlist: Seq[KeyValue] = List() //存储多个列
      var kv: KeyValue = null
      val cf: Array[Byte] = familyName.getBytes //列族
      val rowKey = Bytes.toBytes(row.getAs[Int]("id").toString.reverse)
      val immutableRowKey: ImmutableBytesWritable = new ImmutableBytesWritable(rowKey)
      for (i <- 0 to (columnsName.length - 1)){
        //将rdd转换成HFile需要的格式,
        //我们上面定义了Hfile的key是ImmutableBytesWritable,
        //那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
        var value: Array[Byte] = null
        try {
          //数据是字符串的都映射成String
          value = Bytes.toBytes(row.getAs[String](columnsName(i)))
        } catch {
          case e: ClassCastException =>
            //出现数据类型转换异常则说明是数字,都映射成BigInt
            value = Bytes.toBytes(row.getAs[BigInt](columnsName(i)) + "")
          case e: Exception =>
            e.printStackTrace()
        }
        //封装KeyValue
        kv = new KeyValue(rowKey, cf, Bytes.toBytes(columnsName(i)), value)
        //将新的kv加在kvlist后面（不能反 需要整体有序）
        kvlist :+ kv
      }
      (immutableRowKey, kvlist)
    })

    //根据result获取hfileRDD
    val hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)] = result
      .flatMapValues(_.iterator)

    //1.hfile文件保存到hdfs指定路径
    hfileRDD
      .sortBy(x => (x._1, x._2.getKeyString), true)
      .saveAsNewAPIHadoopFile(hFilePath,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],hbconf)

    //2.HFile导入到HBase
    //开始即那个HFile导入到Hbase,此处都是hbase的api操作
    val bulkLoader: LoadIncrementalHFiles = new LoadIncrementalHFiles(hbconf)
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn: Connection = ConnectionFactory.createConnection(hbconf)
    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    //获取hbase表的region分布
    val regionLocator: RegionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
    //开始bulkLoader导入
    bulkLoader.doBulkLoad(new Path(hFilePath), admin, table, regionLocator)

    hbaseConn.close()
    fileSystem.close()
    spark.stop()

  }

  case class Account(unified_code: String,org_no: String,zhmc: String,hqckzh: String,yxjgmc: String)


}
