package com.study.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount(windowEnd: Long, uvCount: Long)

object UniqueVisitor {
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
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    processedStream.print()
    env.execute("uv job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, uvCount, TimeWindow] {
  //AllWindowFunction[IN, OUT, W <: Window]
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[uvCount]): Unit = {
    // 定义一个scala set，用于保存所有的数据userId并去重
    var idSet = Set[Long]()
    // 把当前窗口所有数据的ID收集到set中，最后输出set的大小,set的集合里面的数据key是不重复的。但set是放在内存里面的，如果数据过大，这种方式就不好用了，后面我们讲讲解布隆过滤期器
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    out.collect(uvCount(window.getEnd, idSet.size))
  }
}
