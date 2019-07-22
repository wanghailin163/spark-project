package com.ibeifeng.sparkproject.test

/**
  * @Description: TODO
  * @Author: wanghailin
  * @Date: 2019/7/22
  */
object Test {

  def main(args: Array[String]): Unit = {

    val range = 1 to 20

    import scala.util.control.Breaks._
    range.foreach(e => {
      breakable{
        if(e==3||e==6){
          break
        }else{
          println(e)
        }
      }

    })

  }

}
