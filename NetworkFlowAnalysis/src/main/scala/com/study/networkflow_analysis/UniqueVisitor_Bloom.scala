package com.study.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

case class uvCount(windowEnd: Long, uvCount: Long)

object UniqueVisitor_Bloom {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2.用相对路径定义数据源
    val resource = getClass.getResource("/txtdata.csv")
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
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      /*
        * 注意 我们要知道窗口关闭后才会触发调用一次函数，如果数据量很大的话，我们需要把内存的数据存到redis,然后把在触发这个操作。
        * 但是我们必须来一条数据，就要存到redis,而不是关闭窗口后，一下子把数据存到redis，不然的话，就相当于没有利用到redis，那就没有必要用redis了，直接就可以窗口里面解决就可以了。
        * 所以我们要调用trigger(..),来触发，这操作，让它每来一条数据就存到redis里面，且每来一次就触发窗口操作。
        */
      .trigger(new MyTrigger())
      .process(new UvCount2WithBloom())

    processedStream.print()
    env.execute("uv with bloom job")
  }
}

// 自定义窗口触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  //EventTime窗口关闭的时候对所有的元素进行操作
  //CONTINUE表示对窗口什么都不做。FIRE表示触发窗口计算，但不会清空窗口的数据。PURGE表示不触发计算，且清楚窗口。FIRE_AND_PURGE前面的合并。而在源码里面关闭窗口是在底层的定时器里面关闭窗口的，而前面介绍的FIRE等常量没有关窗功能
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //ProcessingTime窗口关闭的时候对所有的元素进行操作
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //窗口关闭之后做clear工作
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  //我们这里没有定义定时器，所以不用关闭窗口
  //每来一个元素到窗口，要做什么工作
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态，注意：这里没有关闭窗口
    TriggerResult.FIRE_AND_PURGE
  }
}

// 定义一个布隆过滤器，也可以使用底层代码的布隆过滤器，(额外拓展：布隆过滤的hash算法有可能会产生hash冲突，可能不一样的字符串或内容，进过hash计算后，会产生一样的hash值，也是有可能的)
class myBloom(size: Long) extends Serializable {
  // 位图的总大小，默认16MB(即用十进制表示为134217728个bit个数，一亿多个bit一般来说是够了,而且这种存放有省内存)，验算：16MB=16 x 1024 x 1024 x 8 =134217728bit。134217728bit转换成二进制1000000000000000000000000000，即1 << 27
  private val cap = if (size > 0) {
    size
  } else {
    1 << 27
  }

  //左移动27位，1右边有27个0
  // 定义hash函数,seed为其实种子
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i) //charAt(i)字符串转字符，然后它们相加的时候，又会进行转换int
    }
    //&计算两边都要装换二进制，计算完后 在装换成long类型
    result & (cap - 1) //如果是默认的大小 则cap - 1=134217727，在装换成二进制与result进行计算。当然也可以直接返回result，如果在&计算的话，效果会好点。
  }
}

class UvCount2WithBloom() extends ProcessWindowFunction[(String, Long), uvCount, String, TimeWindow] {
  //ProcessWindowFunction[IN, OUT, KEY, W <: Window]
  // 定义redis连接
  lazy val jedis = new Jedis("192.168.149.111", 6379)
  lazy val bloom = new myBloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[uvCount]): Unit = {

    // 访问redis的windowEndTable，即位图
    // key是windowEndTable，value是bitmap，我们这里不是用哈希的创建方式
    val windowEndTable = context.window.getEnd.toString //redis 不用创建表，连接的时候，会自动创建表

    // 访问redis的countTable
    // 把每个窗口的uvCount值也存入名为countTable的redis表，注意我们用的是哈希的创建方式，存放内容为，注意 K(countTable),V(k:windowEnd ， v:uvCount) 所以要先从redis中读取
    val countTable = "countTable"
    var count = 0L
    if (jedis.hget(countTable, windowEndTable) != null) {
      count = jedis.hget(countTable, windowEndTable).toLong
    }

    // 用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    //由于抽空就一条数据，所以我们直接获取last最后一条数据
    val offset = bloom.hash(userId, 61)
    //61自定义
    val isExist = jedis.getbit(windowEndTable, offset) //传入offset标识位，判断reids位图中有没有这一位,这是一种简便的方法，我们不需要获取value,然后转二进制进行比较，直接有底层提供的方法实现就好
    if (!isExist) {
      // 如果不存在，windowEndTable即位图，对应"位0"设置"位1"，且countTable设置count + 1
      jedis.setbit(windowEndTable, offset, true) //0表示为false，非0表示为true
      jedis.hset(countTable, windowEndTable, (count + 1).toString)
      out.collect(uvCount(windowEndTable.toLong, count + 1))
    } else {
      out.collect(uvCount(windowEndTable.toLong, count))
    }
  }
}

/**
  * 虽然我们用的使用了布隆过滤,当频繁的和reids交互，有点延迟，即时间换内存。
  * 我们可以利用两者之间的综合。当然进行适当的改进，省的，每条数据来都要交互redis内存。
  */

