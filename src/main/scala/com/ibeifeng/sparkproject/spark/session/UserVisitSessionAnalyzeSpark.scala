package com.ibeifeng.sparkproject.spark.session

import java.lang
import java.util.{Date, Random}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.ibeifeng.sparkproject.conf.ConfigurationManager
import com.ibeifeng.sparkproject.constant.Constants
import com.ibeifeng.sparkproject.dao.factory.DAOFactory
import com.ibeifeng.sparkproject.domain._
import com.ibeifeng.sparkproject.test.MockData
import com.ibeifeng.sparkproject.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
  * @Description: 用户访问session分析Spark作业
  *               * 接收用户创建的分析任务，用户可能指定的条件如下：
  *               *
  *               * 1、时间范围：起始日期~结束日期
  *               * 2、性别：男或女
  *               * 3、年龄范围
  *               * 4、职业：多选
  *               * 5、城市：多选
  *               * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
  *               * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
  *               *
  *               * 我们的spark作业如何接受用户创建的任务？
  *               *
  *               * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
  *               * 字段中
  *               *
  *               * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
  *               * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
  *               * 参数就封装在main函数的args数组中
  *               *
  *               * 这是spark本身提供的特性
  * @Author: wanghailin
  * @Date: 2019/4/11
  */
object UserVisitSessionAnalyzeSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
      //.setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CategorySortKey]))

    var sc = new SparkContext(conf)

    val sparkSession = getSQLContext()

    //mockData(sc,sparkSession)

    // 创建需要使用的DAO组件
    val taskDAO = DAOFactory.getTaskDAO()

    //首先得查询出来指定的任务，并获取任务的查询参数
    val taskid = ParamUtils.getTaskIdFromArgs(args)
    val task = taskDAO.findById(taskid)
    val taskParam = JSON.parseObject(task.getTaskParam())


    // 如果要进行session粒度的数据聚合
    // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据

    /**
      * actionRDD,就是一个公共的RDD
      * 第一，要用actionRDD，获取到一个公共的sessionid为key的RDD
      * 第二，actionRDD,用在了session聚合环节里面
      * sessionid为key的RDD是确定了的，但是要在后面多次使用
      * 1.与通过筛选的sessionid进行join,获取通过赛选的session明细数据
      * 2.将这个RDD，直接传如aggregateBySession方法进行session聚合统计
      *
      * 重构完以后，actionRDD,就可以在最开始，使用一次用来以sessionid为key的RDD
      */
    val actionRDD = getActionRDDByDateRange(sparkSession,taskParam)
    var sessionid2actionRDD = getSessionid2ActionRDD(actionRDD)

    /**
      * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
      *
      * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
      * StorageLevel.MEMORY_ONLY_SER()，第二选择
      * StorageLevel.MEMORY_AND_DISK()，第三选择
      * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
      * StorageLevel.DISK_ONLY()，第五选择
      *
      * 如果内存充足，要使用双副本高可靠机制
      * 选择后缀带_2的策略
      * StorageLevel.MEMORY_ONLY_2()
      *
      */
    sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY)

    // 首先，可以将行为数据，按照session_id进行groupByKey分组
    // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
    // 与用户信息数据，进行join
    // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
    // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
    val sessionid2AggrInfoRDD = aggregateBySession(sparkSession,sessionid2actionRDD)

    // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
    // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
    // 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的
    //自定义的累加器
    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator
    sc.register(sessionAggrStatAccumulator,"sessionAggrStatAccumulator")
    // 重构，同时进行过滤和统计
    var filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator)
    filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY)

    //生成公共的RDD：通过筛选条件的session的访问明细数据
    var sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2actionRDD)
    sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY)

    /**
      * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
      * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
      * 再进行。。。
      * 如果没有action的话，那么整个程序根本不会运行。。。
      * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
      * 不对！！！
      * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
      * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
      */
    randomExtractSession(sc,task.getTaskId,filteredSessionid2AggrInfoRDD,sessionid2actionRDD)//countByKey是action算子

    /**
      * 特别说明
      * 我们知道，要将上一个功能的session聚合统计数据获取到，就必须是在一个action操作触发job之后
      * 才能从Accumulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
      * 所以，我们在这里，将随机抽取的功能的实现代码，放在session聚合统计功能的最终计算和写库之前
      * 因为随机抽取功能中，有一个countByKey算子，是action操作，会触发job
      */

    // 计算出各个范围的session占比，并写入MySQL
    calculateAndPersistAggrStat(sessionAggrStatAccumulator.value, task.getTaskId)

    /**
      * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
      *
      * 如果不进行重构，直接来实现，思路：
      * 1、actionRDD，映射成<sessionid,Row>的格式
      * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
      * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
      * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
      * 5、将最后计算出来的结果，写入MySQL对应的表中
      *
      * 普通实现思路的问题：
      * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
      * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
      * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
      *
      * 重构实现思路：
      * 1、不要去生成任何新的RDD（处理上亿的数据）
      * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
      * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
      * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
      * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
      * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
      * 		半个小时，或者数个小时
      *
      * 开发Spark大型复杂项目的一些经验准则：
      * 1、尽量少生成RDD
      * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
      * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
      * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
      * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
      * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
      * 4、无论做什么功能，性能第一
      * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
      * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
      *
      * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
      * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
      * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
      * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
      * 		此时，对于用户体验，简直就是一场灾难
      *
      * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
      *
      * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
      * 		如果采用第二种方案，那么其实就是性能优先
      *
      * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
      * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
      * 		积累了，处理各种问题的经验
      *
      */

    // 获取top10热门品类
    var top10CategoryList = getTop10Category(task.getTaskId,filteredSessionid2AggrInfoRDD, sessionid2detailRDD)
    //获取top10活跃session
    getTop10Session(sc, task.getTaskId,top10CategoryList, sessionid2detailRDD)

    //关闭sparkSession
    sparkSession.close()
  }

  /**
    * 获取SparkSession
    * 如果是在本地测试环境的话，那么就生成SQLContext对象
    * 如果是在生产环境运行的话，那么就生成HiveContext对象
    */
  def getSQLContext():SparkSession={
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local){
      SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).getOrCreate()
    }else{
      SparkSession.builder().enableHiveSupport().appName(Constants.SPARK_APP_NAME_SESSION).getOrCreate()
    }
  }

  /**
    * 生成模拟数据（只有本地模式，才会去生成模拟数据）
    * @param sc
    * @param sqlContext
    */
  def mockData(sc: SparkContext,sparkSession: SparkSession):Unit={
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local){
      MockData.mock(sparkSession)
    }
  }

  /**
    * 获取指定日期范围内的用户访问行为数据
    * @param sparkSession
    * @param taskParam 任务参数
    * @return 行为数据RDD
    */
  def getActionRDDByDateRange(sparkSession: SparkSession,taskParam: JSONObject):RDD[Row]={
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='"+startDate+"' and date<='"+endDate+"'"

    val actionDF = sparkSession.sql(sql)
    actionDF.rdd
  }

  /**
    * 获取sessionid2到访问行为数据的映射的RDD
    * @param actionRDD
    * @return
    */
  def getSessionid2ActionRDD(actionRDD: RDD[Row]):RDD[(String,Row)] = {
    actionRDD.map(row => (row.getAs[String]("session_id"),row))
  }

  /**
    * 对行为数据按session粒度进行聚合
    * @param actionRDD 行为数据RDD
    * @return session粒度聚合数据  <sessionid,fullAggrInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,age,professional,city,sex)>
    */
  def aggregateBySession(sparkSession: SparkSession,sessionid2ActionRDD: RDD[(String,Row)]):RDD[(String,String)]={

    // 现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
    // 我们现在需要将这个Row映射成<sessionid,Row>的格式
    //val sessionid2ActionRDD: RDD[(String, Row)] = actionRDD.map(row => (row.getAs[String]("session_id"),row))

    // 对行为数据按session粒度进行分组
    val sessionid2ActionsRDD: RDD[(String, Iterable[Row])] = sessionid2ActionRDD.groupByKey()

    // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
    // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength)>
    val userid2PartAggrInfoRDD: RDD[(Long, String)] = sessionid2ActionsRDD.map(tuple => {

      var sessionid = tuple._1
      var iterable = tuple._2.iterator
      var searchKeywordsBuffer = new StringBuffer("")
      var clickCategoryIdsBuffer = new StringBuffer("")

      var userid = 0L

      //sessiond的起始和结束时间
      var startTime=new Date()
      var endTime=new Date()
      //session的访问步长
      var stepLength=0

      while (iterable.hasNext) {
        // 提取每个访问行为的搜索词字段和点击品类字段
        var row = iterable.next()
        if (userid == 0L) {
          userid = row.getAs[Long]("user_id")
        }

        var search_keyword = row.getAs[String]("search_keyword")
        var click_category_id = row.getAs[Long]("click_category_id")

        // 实际上这里要对数据说明一下
        // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
        // 其实，只有搜索行为，是有searchKeyword字段的
        // 只有点击品类的行为，是有clickCategoryId字段的
        // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

        // 我们决定是否将搜索词或点击品类id拼接到字符串中去
        // 首先要满足：不能是null值
        // 其次，之前的字符串中还没有搜索词或者点击品类id
        if (StringUtils.isNotEmpty(search_keyword)) {
          if (!searchKeywordsBuffer.toString.contains(search_keyword)) {
            searchKeywordsBuffer.append(search_keyword + ",")
          }
        }

        if (click_category_id != null) {
          if (!clickCategoryIdsBuffer.toString.contains(String.valueOf(click_category_id))) {
            clickCategoryIdsBuffer.append(click_category_id + ",")
          }
        }

        //计算session开始和结束时间
        var actionTime = DateUtils.parseTime(row.getAs[String]("action_time"))
        if(startTime==null){
          startTime=actionTime
        }
        if(endTime==null){
          endTime=actionTime
        }
        if(actionTime.before(startTime)){
          startTime=actionTime
        }
        if(endTime.after(endTime)){
          endTime=actionTime
        }
        // 计算session访问步长
        stepLength+=1
      }

      val searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString())
      val clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString())

      //计算session访问时长（秒）
      var visitLength = (endTime.getTime() - startTime.getTime()) / 1000

      // 我们返回的数据格式，即使<sessionid,partAggrInfo>
      // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
      // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
      // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
      // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
      // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

      // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
      // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
      // 然后再直接将返回的Tuple的key设置成sessionid
      // 最后的数据格式，还是<sessionid,fullAggrInfo>

      // 聚合数据，用什么样的格式进行拼接？
      // 我们这里统一定义，使用key=value|key=value
      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength

      (userid, partAggrInfo)
    })

    //查询所有用户数据，并映射成<userid,Row>的格式
    val sql = "select * from user_info"
    val userInfoRDD = sparkSession.sql(sql).rdd
    val userid2InfoRDD = userInfoRDD.map(row => (row.getAs[Long]("user_id"),row))

    // 将session粒度聚合数据，与用户信息进行join   <userid,<partAggrInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength),userRDD>>
    val userid2FullInfoRDD: RDD[(Long, (String, Row))] = userid2PartAggrInfoRDD.join(userid2InfoRDD)

    // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    val sessionid2FullAggrInfoRDD = userid2FullInfoRDD.map(tuple => {
        var partAggrInfo = tuple._2._1
        var userInfoRow = tuple._2._2

        var sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID)
        var age = userInfoRow.getAs[Int]("age")
        var professional = userInfoRow.getAs[String]("professional")
        var city =  userInfoRow.getAs[String]("city")
        var sex = userInfoRow.getAs[String]("sex")

        var fullAggrInfo = partAggrInfo+"|"+
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_CITY + "=" + city + "|" +
          Constants.FIELD_SEX + "=" + sex

        (sessionid,fullAggrInfo)
    })
    sessionid2FullAggrInfoRDD
  }

  /**
    * 过滤session数据，并进行聚合统计
    * @param sessionid2AggrInfoRDD <sessionid,fullAggrInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,age,professional,city,sex)>
    * @return 过滤后的<sessionid,fullAggrInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,age,professional,city,sex)>
    */
  def filterSessionAndAggrStat(sessionid2AggrInfoRDD: RDD[(String,String)], taskParam: JSONObject,sessionAggrStatAccumulator: SessionAggrStatAccumulator):RDD[(String,String)]={

    // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
    // 此外，这里其实大家不要觉得是多此一举
    // 其实我们是给后面的性能优化埋下了一个伏笔
    val startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)//最小年龄
    val endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)//最大年龄
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)//职业
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)//城市
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)//性别
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)//搜索关键词
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)//品类

    var _parameter = new String
    if(startAge!=null) _parameter+Constants.PARAM_START_AGE + "=" + startAge + "|" else _parameter+Constants.PARAM_START_AGE + "=" + "" + "|"
    if(endAge!=null) _parameter+Constants.PARAM_END_AGE + "=" + endAge + "|" else _parameter+Constants.PARAM_END_AGE + "=" + "" + "|"
    if(professionals!=null) _parameter+Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else _parameter+Constants.PARAM_PROFESSIONALS + "=" + "" + "|"
    if(cities!=null) _parameter+Constants.PARAM_CITIES+ "=" + cities + "|" else _parameter+Constants.PARAM_CITIES+ "=" + "" + "|"
    if(sex!=null) _parameter+Constants.PARAM_SEX+ "=" + sex + "|" else _parameter+Constants.PARAM_SEX+ "=" + "" + "|"
    if(keywords!=null) _parameter+Constants.PARAM_KEYWORDS+ "=" + keywords + "|" else _parameter+Constants.PARAM_KEYWORDS+ "=" + "" + "|"
    if(categoryIds!=null) _parameter+Constants.PARAM_CATEGORY_IDS+ "=" + categoryIds else _parameter+Constants.PARAM_CATEGORY_IDS+ "=" + ""
    if(_parameter.endsWith("\\|")){
      _parameter = _parameter.substring(0,_parameter.length() - 1)
    }

    val parameter = _parameter

    sessionid2AggrInfoRDD.foreach(tuple => {
      println(tuple._1+"-----"+tuple._2)
    })

    val filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(tuple => {
      // 首先，从tuple中，获取聚合数据
      var aggrInfo= tuple._2

      // 接着，依次按照筛选条件进行过滤
      // 按照年龄范围进行过滤（startAge、endAge）
      if(!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
        false
      }

      // 按照职业范围进行过滤（professionals）
      // 互联网,IT,软件
      // 互联网
      if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter, Constants.PARAM_PROFESSIONALS)){
        false
      }

      // 按照城市范围进行过滤（cities）
      // 北京,上海,广州,深圳
      // 成都
      if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)){
        false
      }

      // 按照搜索词进行过滤
      // 我们的session可能搜索了 火锅,蛋糕,烧烤
      // 我们的筛选条件可能是 火锅,串串香,iphone手机
      // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
      // 任何一个搜索词相当，即通过
      if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
         false
      }

      // 按照点击品类id进行过滤
      if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
        false
      }

      // 如果经过了之前的多个过滤条件之后，程序能够走到这里
      // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
      // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
      // 进行相应的累加计数

      //统计赛选范围内session总数量
      sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

      //计算出session的访问时长的范围，并进行相应的累加
      import java.lang.Long
      var visitLength =Long.valueOf(StringUtils.getFieldFromConcatString(
      								aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH))
      if(visitLength >=1 && visitLength <= 3) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
      } else if(visitLength >=4 && visitLength <= 6) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
      } else if(visitLength >=7 && visitLength <= 9) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
      } else if(visitLength >=10 && visitLength <= 30) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
      } else if(visitLength > 30 && visitLength <= 60) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
      } else if(visitLength > 60 && visitLength <= 180) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
      } else if(visitLength > 180 && visitLength <= 600) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
      } else if(visitLength > 600 && visitLength <= 1800) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
      } else if(visitLength > 1800) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)
      }

      //计算出session的访问步长的范围，并进行相应的累加
      var stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
        aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH))
      if(stepLength >= 1 && stepLength <= 3) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
      } else if(stepLength >= 4 && stepLength <= 6) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
      } else if(stepLength >= 7 && stepLength <= 9) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
      } else if(stepLength >= 10 && stepLength <= 30) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
      } else if(stepLength > 30 && stepLength <= 60) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
      } else if(stepLength > 60) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
      }
      true
    })
    filteredSessionid2AggrInfoRDD
  }

  /**
    * 获取通过筛选条件的session的访问明细数据RDD
    * @param sessionid2aggrInfoRDD
    * @param sessionid2actionRDD
    * @return
    */
  def getSessionid2detailRDD(sessionid2aggrInfoRDD: RDD[(String,String)],sessionid2actionRDD: RDD[(String,Row)]):RDD[(String, Row)]={
    sessionid2aggrInfoRDD
      .join(sessionid2actionRDD)
      .map(tuple => (tuple._1, tuple._2._2))
  }

  /**
    * 随机抽取session
    * @param sessionid2AggrInfoRDD 过滤后的<sessionid,fullAggrInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,age,professional,city,sex)>
    */
  def randomExtractSession(sc: SparkContext,taskid: Long,sessionid2AggrInfoRDD: RDD[(String,String)],sessionid2actionRDD: RDD[(String,Row)]):Unit = {
    // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,sessionid>格式的RDD
    var time2sessionidRDD = sessionid2AggrInfoRDD.map(tuple => {
          var fullaggrInfo = tuple._2
          var startTime = StringUtils.getFieldFromConcatString(fullaggrInfo,"\\|",Constants.FIELD_START_TIME)
          if(startTime!=null){
            var dateHour = DateUtils.getDateHour(startTime)
            (dateHour,fullaggrInfo)
          }else{
            ("_",fullaggrInfo)
          }
        })

    /**
      * 思考一下：这里我们不要着急写大量的代码，做项目的时候，一定要用脑子多思考
      *
      * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
      * 首先抽取出的session的聚合数据，写入session_random_extract表
      * 所以第一个RDD的value，应该是session聚合数据
      *
      */

    // 得到每天每小时的session数量
    var countMap: collection.Map[String, Long] = time2sessionidRDD.countByKey()

    // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
    // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    val dateHourCountMap = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Long]]()

    for((dateHour,countNumber) <- countMap){
      var dateHour_ = dateHour
      var date = dateHour_.split("_")(0)
      var hour = dateHour_.split("_")(1)

      var count = countNumber
      var hourCountMap = dateHourCountMap.get(date).get
      if(hourCountMap==null){
        hourCountMap = new scala.collection.mutable.HashMap[String,Long]
        dateHourCountMap.put(date,hourCountMap)
      }
      hourCountMap.put(hour,count)
    }

    //开始实现我们按照时间比例随机抽取算法

    //总共要抽取100个session,先按照天数再进行平分
    var extractNumberPerDay: Int=100/dateHourCountMap.size

    //<date,<hour,抽取session的下标索引(3,5,20,102)>>

    /**
      * session随机抽取功能
      * 用到了一个比较大的变量，随机抽取索引map
      * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
      * 还是比较消耗内存和网络传输性能的
      * 将map做成广播变量
      */
    var dateHourExtractMap = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,ListBuffer[Int]]]

    var random = new Random()
    //遍历<yyyy-MM-dd,<HH,count>>集合
    for((date,hourCountMap) <- dateHourCountMap){
      var date_ = date
      var hourCountMap_ = hourCountMap

      //计算出这一天的session数量
      var sessionCount = 0L
      for((hour,hourCount)<-hourCountMap_){
        sessionCount += hourCount
      }

      var hourExtractMap = dateHourExtractMap.get(date_).get  //<hour,(3,5,20,102)>
      if(hourExtractMap==null){
        hourExtractMap = new scala.collection.mutable.HashMap[String,ListBuffer[Int]]
        dateHourExtractMap.put(date_, hourExtractMap)
      }

      //遍历每个小时<HH,count>
      for((hour,count)<-hourCountMap_){
        var hour_ = hour
        var count_ = count

        // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
        // 就可以计算出，当前小时需要抽取的session数量
        var hourExtractNumber = ((count_.asInstanceOf[Double]/sessionCount.asInstanceOf[Double])*extractNumberPerDay).asInstanceOf[Int]
        if(hourExtractNumber>count_){
          hourExtractNumber = count_.asInstanceOf[Int]
        }

        // 先获取当前小时的存放随机数的list
        var extractIndexList = hourExtractMap.get(hour_).get
        if(extractIndexList==null){
          extractIndexList = new ListBuffer[Int]
          hourExtractMap.put(hour_, extractIndexList)
        }

        // 生成上面计算出来的数量的随机数
        for(i <- 0 to hourExtractNumber){ //0到每小时抽取的数量
          var extractIndex = random.nextInt(count_.asInstanceOf[Int])
          while(extractIndexList.contains(extractIndex)){
            extractIndex = random.nextInt(count_.asInstanceOf[Int])
          }
          extractIndexList+=extractIndex
        }
      }
    }

    /**
      * 广播变量，很简单
      * 其实就是SparkContext的broadcast()方法，传入你要广播的变量，即可
      */
    var dateHourExtractMapBroadcast = sc.broadcast(dateHourExtractMap)


    /**
      * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
      */
    // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>  <yyyy-MM-dd_HH,fullaggrInfo>
    var time2sessionsRDD: RDD[(String,Iterable[String])] = time2sessionidRDD.groupByKey()
    // 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
    // 然后呢，会遍历每天每小时的session
    // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
    // 那么抽取该session，直接写入MySQL的random_extract_session表
    // 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
    // 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表

    var extractSessionidsRDD =
      time2sessionsRDD.flatMap(tuple => {
        var extractSessionids = new ListBuffer[Tuple2[String,String]]
        var dateHour = tuple._1
        var date = dateHour.split("_")(0)
        var hour = dateHour.split("_")(1)
        var iterator: Iterator[String]= tuple._2.iterator

        /**
          * 使用广播变量的时候
          * 直接调用广播变量（Broadcast类型）的value() / getValue()
          * 可以获取到之前封装的广播变量
          */
        var dateHourExtractMap = dateHourExtractMapBroadcast.value
        var extractIndexList: ListBuffer[Int] = dateHourExtractMap.get(date).get(hour)    //每天每小时抽取的session索引集合

        var sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO()

        var index = 0
        while(iterator.hasNext){
          var sessionAggrInfo = iterator.next()

          if(extractIndexList.contains(index)){
            var sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

            // 将数据写入MySQL
            var sessionRandomExtract = new SessionRandomExtract()
            sessionRandomExtract.setTaskId(taskid)
            sessionRandomExtract.setSessionId(sessionid)
            sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME))
            sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS))
            sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS))

            sessionRandomExtractDAO.insert(sessionRandomExtract)
            // 将sessionid加入list
            extractSessionids+=new Tuple2(sessionid,sessionid)
          }
          index+=1
        }
        extractSessionids
    })

    /**
      * 第四步：获取抽取出来的session的明细数据
      */
    var extractSessionDetailRDD:RDD[(String,(String,Row))] = extractSessionidsRDD.join(sessionid2actionRDD)

    extractSessionDetailRDD.foreach(tuple => {
      var row = tuple._2._2

      var sessionDetail = new SessionDetail()
      sessionDetail.setTaskId(taskid)
      sessionDetail.setUserId(row.getAs[Long]("user_id"))
      sessionDetail.setSessionId(row.getAs[String]("session_id"))
      sessionDetail.setPageId(row.getAs[Long]("page_id"))
      sessionDetail.setActionTime(row.getAs[String]("action_time"))
      sessionDetail.setSearchKeyword(row.getAs[String]("search_keyword"))
      sessionDetail.setClickCategoryId(row.getAs[Long]("click_category_id"))
      sessionDetail.setClickProductId(row.getAs[Long]("click_product_id"))
      sessionDetail.setOrderCategoryIds(row.getAs[String]("order_category_ids"))
      sessionDetail.setOrderProductIds(row.getAs[String]("order_product_ids"))
      sessionDetail.setPayCategoryIds(row.getAs[String]("pay_category_ids"))
      sessionDetail.setPayProductIds(row.getAs[String]("pay_product_ids"))

      var sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insert(sessionDetail)
    })

  }

  /**
    * 计算各session范围占比，并写入MySQL
    * @param value
    * @param taskid
    */
  def calculateAndPersistAggrStat(value: String,taskid: lang.Long):Unit={
    // 从Accumulator统计串中获取值
    val session_count = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT))

    val visit_length_1s_3s = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s))
    val visit_length_4s_6s = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s))
    val visit_length_7s_9s = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s))
    val visit_length_10s_30s = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s))
    val visit_length_30s_60s = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s))
    val visit_length_1m_3m = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m))
    val visit_length_3m_10m = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m))
    val visit_length_10m_30m = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m))
    val visit_length_30m = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m))

    val step_length_1_3 = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3))
    val step_length_4_6 = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6))
    val step_length_7_9 = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9))
    val step_length_10_30 = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30))
    val step_length_30_60 = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60))
    val step_length_60 = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60))

    // 计算各个访问时长和访问步长的范围// 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.asInstanceOf[Double] / session_count.asInstanceOf[Double], 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = new SessionAggrStat()
    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

    // 调用对应的DAO插入统计结果// 调用对应的DAO插入统计结果

    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)
  }

  //<sessionid,fullAggrInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,age,professional,city,sex)>
  /**
    * 获取top10热门品类
    * @param filteredSessionid2AggrInfoRDD <sessionid,fullAggrInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,age,professional,city,sex)>
    * @param sessionid2actionRDD <sessionid,Row>
    */
  def getTop10Category(taskId: Long,
                       filteredSessionid2AggrInfoRDD: RDD[(String,String)],
                       sessionid2actionRDD: RDD[(String, Row)]):Array[(CategorySortKey, String)] = {
    /**
      * 第一步：获取符合条件的session访问过的所有品类
      */

    // 获取符合条件的session的访问明细
    val sessionid2detailRDD = filteredSessionid2AggrInfoRDD
      .join(sessionid2actionRDD)
      .map(tuple => {
        (tuple._1,tuple._2._2)   //过滤后的<session,Row>
      })

    // 获取session访问过的所有品类id
    // 访问过：指的是，点击过、下单过、支付过的品类
    var categoryidRDD = sessionid2detailRDD
      .flatMap(tuple => {
        var row = tuple._2
        var list = new ListBuffer[(Long,Long)]

        //点击过品类
        var clickCategoryId = row.getAs[Long]("click_category_id")
        if(clickCategoryId!=null){
          list+=new Tuple2(clickCategoryId,clickCategoryId)
        }

        //下单过品类
        var orderCategoryIds = row.getAs[String]("order_category_ids")
        if(orderCategoryIds!=null){
          var orderCategoryIdsSplited = orderCategoryIds.split(",")
          for(orderCategoryId <- orderCategoryIdsSplited){
            list+=new Tuple2(orderCategoryId.asInstanceOf[Long],orderCategoryId.asInstanceOf[Long])
          }
        }

        //支付过品类
        var payCategoryIds = row.getAs[String]("pay_category_ids")
        if(payCategoryIds!=null){
          var payCategoryIdsSplited = payCategoryIds.split(",")
          for(payCategoryId <- payCategoryIdsSplited){
            list+=new Tuple2(payCategoryId.asInstanceOf[Long],payCategoryId.asInstanceOf[Long])
          }
        }
        list
    })

    /**
      * 必须要进行去重
      * 如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid已经countInfo进行排序
      * 最后很可能会拿到重复的数据
      */
    categoryidRDD = categoryidRDD.distinct()

    /**
      * 第二步：计算各品类的点击、下单和支付的次数
      */

    // 访问明细中，其中三种访问行为是：点击、下单和支付
    // 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
    // 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算

    // 计算各个品类的点击次数
    var clickCategoryId2CountRDD: RDD[(Long, Long)] = getClickCategoryId2CountRDD(sessionid2detailRDD)
    // 计算各个品类的下单次数
    var orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD)
    // 计算各个品类的支付次数
    var payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD)


    /**
      * 第三步：join各品类与它的点击、下单和支付的次数
      *
      * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id  <categoryid,categoryid>
      *
      * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，可能不是包含所有品类的
      * 比如，有的品类，就只是被点击过，但是没有人下单和支付
      *
      * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRDD不能
      * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRDD还是要保留下来的
      * 只不过，没有join到的那个数据，就是0了
      *
      */
    //categoryid2countRDD里面 <categoryid,countString>
    var categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD)

    /**
      * 第四步：自定义二次排序key
      */

    /**
      * 第五步：将数据映射成<CategorySortKey,countInfo>格式的RDD，然后进行二次排序（降序）
      */
    var sortKey2countRDD = categoryid2countRDD
      .map(tuple =>{
          var countInfo = tuple._2
          var clickCount = StringUtils.getFieldFromConcatString(
            countInfo, "\\|", Constants.FIELD_CLICK_COUNT)
          var orderCount = StringUtils.getFieldFromConcatString(
            countInfo, "\\|", Constants.FIELD_ORDER_COUNT)
          var payCount = StringUtils.getFieldFromConcatString(
            countInfo, "\\|", Constants.FIELD_PAY_COUNT)

          var sortKey = new CategorySortKey(clickCount.asInstanceOf[Int],
            orderCount.asInstanceOf[Int], payCount.asInstanceOf[Int])

          (sortKey,countInfo)
    })

    sortKey2countRDD.sortByKey(false)//降序排序

    /**
      * 第六步：用take(10)取出top10热门品类，并写入MySQL
      */
    var top10CategoryDAO = DAOFactory.getTop10CategoryDAO()
    var top10CategoryList: Array[(CategorySortKey, String)] = sortKey2countRDD.take(10)

    for(top10Category <- top10CategoryList){
      var countInfo = top10Category._2
      var categoryid = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_CATEGORY_ID)//品类id
      var clickCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_CLICK_COUNT)//点击次数
      var orderCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_ORDER_COUNT)//下单次数
      var payCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_PAY_COUNT)//支付次数

      var category = new Top10Category()
      category.setTaskid(taskId)
      category.setCategoryid(categoryid.asInstanceOf[Long])
      category.setClickCount(clickCount.asInstanceOf[Long])
      category.setOrderCount(orderCount.asInstanceOf[Long])
      category.setPayCount(payCount.asInstanceOf[Long])

      top10CategoryDAO.insert(category)
    }
    top10CategoryList
  }


  /**
    * 获取各品类点击次数RDD
    * @param sessionid2detailRDD
    * @return
    */
  def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String,Row)]):RDD[(Long,Long)]={
    var clickCategoryId2CountRDD = sessionid2detailRDD
      .filter(tuple =>{tuple._2.getAs[Long]("click_category_id")!=null})
      .map(tuple => (tuple._2.getAs[Long]("click_category_id"),1L))
      .reduceByKey(_+_)
    clickCategoryId2CountRDD
  }

  /**
    * 获取各品类的下单次数RDD
    * @param sessionid2detailRDD
    * @return
    */
  def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String,Row)]):RDD[(Long,Long)]={
    sessionid2detailRDD
      .filter(tuple => tuple._2.getAs[String]("order_category_ids")!=null)
      .flatMap(tuple => {
        var orderCategoryIds = tuple._2.getAs[String]("order_category_ids")
        var orderCategoryIdsSplited = orderCategoryIds.split(",")
        var list = new ListBuffer[(Long,Long)]
        for(orderCategoryId <- orderCategoryIdsSplited){
          list+=new Tuple2(orderCategoryId.asInstanceOf[Long],1L)
        }
        list
      })
      .reduceByKey(_+_)
  }

  /**
    * 获取各个品类的支付次数RDD
    * @param sessionid2detailRDD
    * @return
    */
  def getPayCategoryId2CountRDD(sessionid2detailRDD: RDD[(String,Row)]):RDD[(Long,Long)]={
    sessionid2detailRDD
      .filter(tuple => tuple._2.getAs[String]("pay_category_ids")!=null)
      .flatMap(tuple => {
        var payCategoryIds = tuple._2.getAs[String]("pay_category_ids")
        var payCategoryIdsSplited = payCategoryIds.split(",")
        var list = new ListBuffer[(Long,Long)]
        for(payCategoryId <- payCategoryIdsSplited){
          list+=new Tuple2(payCategoryId.asInstanceOf[Long],1L)
        }
        list
      })
      .reduceByKey(_+_)
  }

  /**
    * 连接品类RDD与数据RDD
    * @param categoryidRDD                访问过的所有品类<CategoryId,CategoryId>
    * @param clickCategoryId2CountRDD     点击过的品类的次数<CategoryId,count>
    * @param orderCategoryId2CountRDD     下单过的品类的次数<CategoryId,count>
    * @param payCategoryId2CountRDD       支付过的品类的次数<CategoryId,count>
    * @return
    */
  def joinCategoryAndData(categoryidRDD: RDD[(Long,Long)],
                          clickCategoryId2CountRDD: RDD[(Long,Long)],
                          orderCategoryId2CountRDD: RDD[(Long,Long)],
                          payCategoryId2CountRDD: RDD[(Long,Long)]):RDD[(Long,String)]={
    // 解释一下，如果用leftOuterJoin，就可能出现，右边那个RDD中，join过来时，没有值
    // 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值
    var tmpJoinRDD: RDD[(Long, (Long, Option[Long]))] = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD)

    var tmpMapRDD: RDD[(Long,String)] = tmpJoinRDD.map(tuple =>{
        var categoryid = tuple._1
        var optional: Option[Long] = tuple._2._2
        var clickCount = 0L
        if(!optional.isEmpty){
          clickCount=optional.get  //如果optional里面不为null就获取值
        }
        var value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (categoryid, value)
    })

    tmpMapRDD = tmpMapRDD
                .leftOuterJoin(orderCategoryId2CountRDD)    //<Long,<String,Option[Long]>>  品类id,<拼接的字符串value，次数>
                .map(tuple =>{
                     var categoryid = tuple._1
                     var value = tuple._2._1
                     var optional: Option[Long] = tuple._2._2
                     var orderCount = 0L
                     if(!optional.isEmpty){
                       orderCount=optional.get  //如果optional里面不为null就获取值
                     }
                     value = value +  "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
                     (categoryid, value)
                })

    tmpMapRDD = tmpMapRDD
                 .leftOuterJoin(payCategoryId2CountRDD)
                 .map(tuple=>{
                      var categoryid = tuple._1
                      var value = tuple._2._1
                      var optional = tuple._2._2
                      var payCount = 0L

                      if (!optional.isEmpty){
                        payCount = optional.get //如果optional里面不为null就获取值
                      }
                      value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
                      (categoryid, value)
                  })

    tmpMapRDD
  }

  /**
    * 获取top10活跃session
    * @param taskid
    * @param top10CategoryList Array[(CategorySortKey, String)]
    * @param sessionid2detailRDD
    */
  def getTop10Session(sc: SparkContext,taskid: Long,top10CategoryList: Array[(CategorySortKey, String)],sessionid2detailRDD: RDD[(String,Row)] ):Unit = {
    /**
      * 第一步：将top10热门品类的id，生成一份RDD
      */
    var top10CategoryIdList = new ListBuffer[(Long,Long)]
    for(category <- top10CategoryList){
      var categoryid = StringUtils.getFieldFromConcatString(
        category._2, "\\|", Constants.FIELD_CATEGORY_ID)//获取top10的品类id
      top10CategoryIdList+=new Tuple2(categoryid.asInstanceOf[Long],categoryid.asInstanceOf[Long])
    }

    var top10CategoryIdRDD = sc.parallelize(top10CategoryIdList)

    /**
      * 第二步：计算top10品类被各session点击的次数
      */
    var sessionid2detailsRDD = sessionid2detailRDD.groupByKey()

    var categoryid2sessionCountRDD = sessionid2detailsRDD
      .flatMap(tuple => {
          var sessionid = tuple._1
          var iterator = tuple._2.iterator

          var categoryCountMap = new scala.collection.mutable.HashMap[Long,Long]

          //计算出该session,对每个品类的点击次数
          while(iterator.hasNext){
            val row = iterator.next()

            if(row.getAs[Long]("click_category_id")!=null){
              var categoryid = row.getAs[Long]("click_category_id")

              var count = categoryCountMap.get(categoryid).get
              if(count ==null){
                count = 0L
              }
              count+=1
              categoryCountMap.put(categoryid, count)
            }
          }

          // 返回结果，<categoryid,   sessionid,count>格式
          var list = new ListBuffer[(Long,String)]

          for((categoryid,count)<-categoryCountMap){
            var categoryid_ = categoryid
            var count_ = count
            var value = sessionid + "," + count_
            list+=new Tuple2(categoryid_, value)
          }

          list
      })

    // 获取到to10热门品类，被各个session点击的次数 top10的<categoryid,  sessionid,count>
    var top10CategorySessionCountRDD = top10CategoryIdRDD   //访问量top10的品类id RDD<categoryid,categoryid>
      .join(categoryid2sessionCountRDD)                     //满足条件的点击的品类 <categoryid,   sessionid,count>
      .map(tuple => (tuple._1, tuple._2._2))


    /**
      * 第三步：分组取TopN算法实现，获取top10的每个品类的top10活跃用户
      */
    //按照categoryid进行聚合
    var top10CategorySessionCountsRDD: RDD[(Long, Iterable[String])] = top10CategorySessionCountRDD.groupByKey()

    val loop = new Breaks

    var top10SessionRDD = top10CategorySessionCountsRDD.flatMap(tuple =>{
      var categoryid = tuple._1
      var iterator = tuple._2.iterator

      //定义取top10的排序数组
      var top10Sessions = new Array[String](10)

      while(iterator.hasNext){
        var sessionCount = iterator.next()
        var count = sessionCount.split(",")(1).asInstanceOf[Long]

        // 遍历排序数组
        loop.breakable {
          for (i <- 0 to top10Sessions.length) {
            // 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
            if (top10Sessions(i) == null) {
              top10Sessions(i) = sessionCount
              loop.break()
            }else{
              var _count = top10Sessions(i).split(",")(1).asInstanceOf[Long]

              //如果如果sessionCount比i位的sessionCount要大
              if(count > _count){
                // 从排序数组最后一位开始，到i位，所有数据往后挪一位
                for(j <- 9 to i){
                  top10Sessions(j) = top10Sessions(j-1)
                }
                // 将i位赋值为sessionCount
                top10Sessions(i) = sessionCount
                loop.break()
              }
              // 比较小，继续外层for循环
            }
          }
        }
      }
      // 将数据写入MySQL表// 将数据写入MySQL表
      val list = new ListBuffer[(String,String)]
      for(sessionCount <- top10Sessions){
        var sessionid = sessionCount.split(",")(0)
        var count = sessionCount.split(",")(1).asInstanceOf[Long]

        // 将top10 session插入MySQL表
        var top10Session = new Top10Session()
        top10Session.setTaskid(taskid);
        top10Session.setCategoryid(categoryid);
        top10Session.setSessionid(sessionid);
        top10Session.setClickCount(count);

        var top10SessionDAO = DAOFactory.getTop10SessionDAO()
        top10SessionDAO.insert(top10Session)
        //放入list
        list+=new Tuple2(sessionid,sessionid)
      }
      list
    })

    /**
      * 第四步：获取top10活跃session的明细数据，并写入MySQL
      */
    var sessionDetailRDD: RDD[(String, (String, Row))] = top10SessionRDD.join(sessionid2detailRDD)
    sessionDetailRDD.foreach(tuple => {
      val row = tuple._2._2
      val sessionDetail = new SessionDetail()
      sessionDetail.setTaskId(taskid)
      sessionDetail.setUserId(row.getAs[Long]("user_id"))
      sessionDetail.setSessionId(row.getAs[String]("session_id"))
      sessionDetail.setPageId(row.getAs[Long]("page_id"))
      sessionDetail.setActionTime(row.getAs[String]("action_time"))
      sessionDetail.setSearchKeyword(row.getAs[String]("search_keyword"))
      sessionDetail.setClickCategoryId(row.getAs[Long]("click_category_id"))
      sessionDetail.setClickProductId(row.getAs[Long]("click_product_id"))
      sessionDetail.setOrderCategoryIds(row.getAs[String]("order_category_ids"))
      sessionDetail.setOrderProductIds(row.getAs[String]("order_product_ids"))
      sessionDetail.setPayCategoryIds(row.getAs[String]("pay_category_ids"))
      sessionDetail.setPayProductIds(row.getAs[String]("pay_product_ids"))
      val sessionDetailDAO = DAOFactory.getSessionDetailDAO()
      sessionDetailDAO.insert(sessionDetail)
    })
  }

}