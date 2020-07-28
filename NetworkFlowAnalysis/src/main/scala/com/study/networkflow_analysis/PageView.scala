package com.study.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2.用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    //我们这里就不使用绝对路径了,用相对路径,注意这里的话还要加个正斜杠，因为获取后面获取绝对路径的时候，后面会拼接上去
    val txtdata = env.readTextFile(resource.getPath)
    //获取绝对路径
    val dataStream = txtdata.map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior == "pv") // 只统计pv操作
      .map(data => ("pv", 1)) //包装二元组
      .keyBy(_._1) //其实这一步keyby看开起来是多此一举，但目的是把它转成keyDataStream,
      .timeWindow(Time.hours(1)) //滚动窗口
      .sum(1) //对元组的第二个元素sum

    processedStream.print()
    env.execute("page view jpb")
  }
}
