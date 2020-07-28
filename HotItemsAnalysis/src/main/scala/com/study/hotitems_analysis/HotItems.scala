package com.study.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1, Tuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 统计热门商品
  *
  * @param userId
  * @param itemId
  * @param categoryId
  * @param behavior
  * @param timestamp
  */
// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. txt读取数据
    val txtData = env.readTextFile("D:\\WorkCache_IDEA\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream = txtData.map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 2. kafka读取数据
    //      val properties = new Properties()
    //      properties.setProperty("bootstrap.servers", "192.168.149.111:9092")
    //      properties.setProperty("group.id", "111")
    //      properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")//注意这里是反序列化，不是序列化
    //      properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //      properties.setProperty("auto.offset.reset", "latest")
    //      val dataStream = env.addSource( new FlinkKafkaConsumer[String]("test2", new SimpleStringSchema(), properties) )
    //        .map( data => {
    //          val dataArray = data.split(",")
    //          UserBehavior( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong )
    //        } )
    //        .assignAscendingTimestamps( _.timestamp * 1000L )

    // 3. transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId) //第一次分流，需要计数每类的商品访问量
      .timeWindow(Time.hours(1), Time.minutes(5)) //长度1h,滑动5分钟
      //new CountAgg(), new WindowResult() 定义窗口聚合规则、定义输出数据结构，返回dataStream，还要给窗口
      .aggregate(new CountAgg(), new WindowResult()) // 窗口聚合 aggregate(preAggregator: AggregateFunction[T输入, ACC中间状态累加, V输出],windowFunction: WindowFunction[V接收前面的输出, R输出类型, K key类型, W]): DataStream[R]。
      .keyBy(_.windowEnd) //第二次 再次分流 ，是因为我们接受的dataStrem,而且接受的数据是统计过的，我们通过之前添加的窗口end标签, 在次分流。（一次分流分流是计算不出结果的，最少两次）
      .process(new TopNHotItems(3))

    // 4. sink：控制台输出
    processedStream.print()

    env.execute("hot items job")
  }
}

// 自定义预聚合函数：计数功能. CountAgg计数累加器
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] { //AggregateFunction<IN输入类型, ACC中间状态, OUT输出类型>
  override def createAccumulator(): Long = 0L //初始值
override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1 //累加
override def merge(a: Long, b: Long): Long = a + b //前后合并,之前的计数结果与本次的计数结果合并。
override def getResult(accumulator: Long): Long = accumulator //获取结果
}

// 自定义预聚合函数：计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] { //我们定义一个元组(Long, Int)， Long是求和的类型， Int的个数的类型。Double是前面两个相除的计算结果
  override def createAccumulator(): (Long, Int) = (0L, 0) //初始值
override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

  //累加，这里原数据用timestamp进行求和
  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)

  //前后合并
  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2 //获取结果
}

// 自定义窗口函数，输出ItemViewCount
class WindowResult() extends WindowFunction[Long /*输入的计数结果类型*/ , ItemViewCount, Long, TimeWindow] { //WindowFunction[IN输入, OUT输出, KEY主键, W窗口 <: Window] ，W是Window下型
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long] /*计数结果*/ , out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next() /*输出计数结果*/)) //这里要跟商品打窗口的时间标签，不然不知道数据是哪一个窗口的。getEnd的单位是秒
  }
}

// 自定义的处理函数
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  //KeyedProcessFunction<K, I, O> ,K是windowsEnd
  private var itemState: ListState[ItemViewCount] = _

  //初始化,当然也可以使用  lazy val =... 创建
  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  //处理每条数据
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value) //注意这里没有itemState.update()，updata是不断的更新覆盖操作，这里是add追加，如果用了update，那最后就一个值。
    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1) //windowEnd是秒的单位，+1表示定时器为1s
  }

  // 定时器触发时，对所有数据排序，并输出结果 ，timestamp是触发计时器的时间戳
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将所有state中的数据取出，放到一个List Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    //遍历状态，要引入这个包，因为itemState取出数据要用get()。
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) { //当然可以使用别的方式提取里面的元素
      allItems += item
    }
    // 清空状态
    itemState.clear()
    //把状态里面的数据都提出来，就可以清空了
    // 按照count大小排序，并取前N个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    //sortBy默认树升序，(Ordering.Long.reverse)表示反转，降序
    // 将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("windowEnd时间：").append(new Timestamp(timestamp - 1)).append("\n") //timestamp - 1 是因为包括了定时器的1s
    // 输出每一个商品的信息
    for (i <- sortedItems.indices) {
      //sortedItems.indices这是一种新的写法，其实跟.0到length-1的效果一样
      val currentItem = sortedItems(i)
      result.append("排行榜top").append(i + 1).append(":") //注意索引是0开始的
        .append(" 热门商品ID=").append(currentItem.itemId)
        .append(" 浏览量计数=").append(currentItem.count)
        .append("\n")
    }
    result.append("================================")
    // 控制输出频率
    Thread.sleep(2000) //远行结果你会看到：数据不断输出，是因为我们是使用的是事件时间，而我们的txt文件，里面的数据很多，时间堆积了很多，所以不不会输出，所以我们我们进行随眠2s,防止输出过快。
    out.collect(result.toString())
  }
}

/**
  * 远行结果你会看到：数据不断，是因为我们是时间堆积成这样的
  */

