package com.study.marketanalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


object AppMarketing {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val dataStream = env.addSource(new SimulatedEventSource1())

    // 3. transform 处理数据
    val processDataStream = dataStream
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ("dummyKey", 1L) //是一个假key: dummyKey
      })
      .keyBy(_._1) // 以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg(), new MarketingCountTotal())

    processDataStream.print()
    env.execute("app marketing job")
  }
}

// 自定义数据源
class SimulatedEventSource1() extends RichSourceFunction[MarketingUserBehavior] {
  // 定义是否运行的标识位
  var running = true
  // 定义用户行为的集合
  val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 定义渠道的集合
  val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 定义一个随机数发生器
  val rand: Random = new Random()

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义一个生成数据的上限
    val maxElements = Long.MaxValue
    var count = 0L
    // 随机生成所有数据
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      //产生的随机数在0~behaviorTypes.size之间
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts = System.currentTimeMillis() //毫秒
      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts)) //输出
      count += 1
      TimeUnit.MILLISECONDS.sleep(10L) //睡眠10ms
    }
  }
}

class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
  //[IN, OUT, KEY, W <: Window]
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    val count = input.iterator.next() //里面就可以一个值，所以直接next()
    out.collect(MarketingViewCount(startTs, endTs, "AppMarketing", "全部计数", count))
  }
}

/**
  * 远行的结果中：数据的计数开始的计数会越来越多，是是因为类似于 数据没有把窗口塞满，到生产到一定的时期的时候，就会生成数据计数比较稳定的状态
  */
