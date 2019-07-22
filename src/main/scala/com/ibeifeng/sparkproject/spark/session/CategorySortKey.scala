package com.ibeifeng.sparkproject.spark.session

/**
  * @Description: 自定义二次排序规则类
  * @Author: wanghailin
  * @Date: 2019/4/20
  */
class CategorySortKey(val clickCount: Int,
                      val orderCount: Int,
                      val payCount: Int) extends Ordered[CategorySortKey] with Serializable{

  override def compare(that: CategorySortKey): Int = {
    if(clickCount - that.clickCount !=0){
      clickCount - that.clickCount
    }else if(orderCount - that.orderCount != 0){
      orderCount - that.orderCount
    }else if(payCount - that.payCount != 0){
      payCount - that.payCount
    }else{
      0
    }
  }
}
