package com.study.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 在之前的，我们使用cep的模式匹配,可以简单的处理超时订单的处理。
  * 现在，我们在用Prosess()，结合状态编程和定时器，来实现下，而且我们要保证可以来一条数据就可以出，不用等定时器到了，在执行，这也相当于模仿cep的初衷
  */

// 定义输入订单事件的样例类
case class OrderEvent1(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult1(orderId: Long, resultMsg: String)

object OrderTimeoutWithoutCep {

  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取订单数据
    //端口读取数据，这里的代码不适合端口读取数据，代码还是有点不完善
    val socketData = env.socketTextStream("localhost", 7777)
    val resource = getClass.getResource("/OrderLog.csv")
    val csvData = env.readTextFile(resource.getPath)
    // 切分
    val orderEventStream = csvData.map(data => {
      val dataArray = data.split(",")
      OrderEvent2(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)

    // 3. transform 处理数据
    val orderResultStream = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatch())

    // 4.sink
    orderResultStream.print("支付成功的订单")
    orderResultStream.getSideOutput(new OrderPayMatch().orderTimeoutOutputTag).print("超时的订单")
    env.execute("order timeout without cep job")
  }

}

class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent2, OrderResult1] {

  //定于侧输出流的标签
  val orderTimeoutOutputTag = new OutputTag[OrderResult1]("orderTimeout")

  // 保存定时器的时间戳为状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))
  // 保存定时器的时间戳为状态
  lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))


  override def processElement(value: OrderEvent2, ctx: KeyedProcessFunction[Long, OrderEvent2, OrderResult1]#Context, out: Collector[OrderResult1]): Unit = {
    // 先读取状态
    val isPayed = isPayedState.value()
    //默认为false
    val timerTs = timerState.value()

    /*
      * (我们这里先定一个规定：create和pay之间、pay和create之间的时间间隔为15分钟内，而源数据也必须符合这情况。
      * 这里的逻辑，比较复杂。这里举例源数据接受会出现的4种情况，可以对应该该4种情况下，分别一一套进去，即可以理解。
      * 注意：
      * 1.第一条数据是create...在第n条数据是终于出现了pay：则会走if( value.eventType == "create" ){ }，接受到pay后在走else if ( value.eventType == "pay" ){ }，计时器会删除
      * 2.第一条数据是pay...在第n条数据是终于出现了create：则会走else if ( value.eventType == "pay" ){ }，接受到create后在走if( value.eventType == "create" ){ }，计时器会删除。
      * 3.第一条数据是create...，后面一直没有出现pay，则会走if( value.eventType == "create" ){ }，计数器会执行，说明是超时支付了，或者是根本没有支付
      * 4.第一条数据是pay...，后面一直没有出现create，则会走else if( value.eventType == "pay" ){ }，计数器会执行,说明是超时了 一直没有接收到create，即create丢失了
      */

    // 根据事件的类型进行分类判断，做不同的处理逻辑
    if (value.eventType == "create") {
      // 1. 如果是create事件，接下来判断pay是否来过
      if (isPayed) {
        //第一个条数据是不会走到这里的
        // 1.1 如果已经pay过，匹配成功，输出主流，清空状态
        out.collect(OrderResult1(value.orderId, "情况一 payed successfully and no timeout. 情况二 payed successfully and timeout.情况三 前面两种都不是")) //这里有多种情况，这里没有进一步的处理，有时间，学者，可以自行处理，比较复杂
        ctx.timerService().deleteEventTimeTimer(timerTs)
        isPayedState.clear()
        timerState.clear()
      } else {
        // 1.2 如果没有pay过，注册定时器等待pay的到来
        val ts = value.eventTime * 1000L + 15 * 60 * 1000L // 15分钟
        ctx.timerService().registerEventTimeTimer(ts)
        timerState.update(ts)
      }
    } else if (value.eventType == "pay") {
      // 2. 如果是pay事件，那么判断是否create过，用timer表示
      if (timerTs > 0) {
        // 2.1 如果有定时器，说明已经有create来过
        // 继续判断，是否超过了timeout时间
        if (timerTs > value.eventTime * 1000L) {
          // 2.1.1 如果定时器时间还没到，那么输出成功匹配
          out.collect(OrderResult1(value.orderId, "payed successfully and no timeout"))
        } else {
          // 2.1.2 如果当前pay的时间已经超时，那么输出到侧输出流
          ctx.output(orderTimeoutOutputTag, OrderResult1(value.orderId, "payed successfully and timeout"))
        }
        // 输出结束，清空状态
        ctx.timerService().deleteEventTimeTimer(timerTs)
        isPayedState.clear()
        timerState.clear()
      } else {
        // 2.2 pay先到了，更新状态，注册定时器等待create
        isPayedState.update(true)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L) //这里为什么要用当前的时间，因为create的是时间本来就在pay的时间之前，所以我们水位线，一旦超过了该定时器的时间，就会触发，其实执行到这里，就会立马触发定时器操作。如果水位线涨到了一定位置，还没有create的来到，说明说明数据是自动丢失了
        timerState.update(value.eventTime * 1000L)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent2, OrderResult1]#OnTimerContext, out: Collector[OrderResult1]): Unit = {
    // 根据状态的值，判断哪个数据没来
    if (isPayedState.value()) {
      // 如果为true，表示pay先到了，没等到create
      ctx.output(orderTimeoutOutputTag, OrderResult1(ctx.getCurrentKey, "already payed ，but not found create log"))
    } else {
      // 表示create到了，没等到pay
      ctx.output(orderTimeoutOutputTag, OrderResult1(ctx.getCurrentKey, "order timeout"))
    }
    isPayedState.clear()
    timerState.clear()
  }

}
