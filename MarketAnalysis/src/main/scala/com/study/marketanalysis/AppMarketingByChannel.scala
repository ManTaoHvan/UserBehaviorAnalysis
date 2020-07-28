package com.study.marketanalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


// 输入数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 输出结果样例类
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //我们事件的时间，其实本地处理时间模拟出来的数据，所以是不乱序的事件时间，

    // 2. 读取数据
    val dataStream = env.addSource(new SimulatedEventSource())

    // 3. transform 处理数据
    val processDataStream = dataStream
      .assignAscendingTimestamps(_.timestamp) //我们传入的时间就是毫秒单位
      .filter(_.behavior != "UNINSTALL") //卸载的话就没有必要算了
      .map(data => {
      ((data.channel, data.behavior), 1L) //这里加了1L，是用于后面的count计数,但在后面的代码没有用上，这里做一个小小的冗余，提供更好的扩展性
    })
      .keyBy(_._1) // 以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel()) //当然我们也可以使用.aggregate()方法，这里我们随便做下process()的处理

    processDataStream.print()
    env.execute("app marketing by channel job")
  }
}

// 自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {
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

// 自定义处理函数
class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size //直接判断窗口的大小就可以了，所以接受的到计数值1，没有用上
    out.collect(MarketingViewCount(startTs, endTs, channel, behavior, count))
  }
}

/**
  * 注意 远行输出的结果中：可能有下载量比点击量还要多，一般来说不符合生活的逻辑。所以这里只是学习使用，不必计较
  */