package com.study.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//源数据：83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico，这里有七个字段，- -表示userid 和username 的省略

// 输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. txt读取数据
    val txtData = env.readTextFile("D:\\WorkCache_IDEA\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    // 2.端口读取数据
    val socketdata = env.socketTextStream("192.168.149.111", 7777)
    val dataStream = txtData
      .map(data => {
        val dataArray = data.split(" ")
        // 定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        //延迟1s
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })

    // 3. transform 处理数据
    val processedStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60)) //延迟，这里没有直接设置在水位线
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    // 4. sink：控制台输出
    processedStream.print()
    env.execute("network flow job")
  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] { //AggregateFunction<IN, ACC, OUT>
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def merge(a: Long, b: Long): Long = a + b

  override def getResult(accumulator: Long): Long = accumulator
}

// 自定义窗口处理函数
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] { //WindowFunction[IN, OUT, KEY, W <: Window]
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义排序输出处理函数
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] { // KeyedProcessFunction<K, I, O> ,K是windowsEnd

  lazy val urlState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("url-state", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //timestamp是触发计时器的时间戳
    // 从状态中拿到数据
    val allUrlViews: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
    //另一种提取元素的方式，当然也可以用get()
    val iter = urlState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allUrlViews += ((entry.getKey, entry.getValue)) //url,count
    }
    urlState.clear()
    val sortedUrlViews = allUrlViews.sortWith(_._2 > _._2).take(topSize)
    //另一种方式，做降序排列，
    // 格式化结果输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n") //timestamp - 1 是因为包括了定时器的1s
    for (i <- sortedUrlViews.indices) {
      val currentUrlView = sortedUrlViews(i)
      result.append("NO").append(i + 1).append(":")
        .append(" URL=").append(currentUrlView._1)
        .append(" 访问量=").append(currentUrlView._2).append("\n")
    }
    result.append("=============================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}