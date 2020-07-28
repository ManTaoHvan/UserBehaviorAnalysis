package com.study.orderpay_detect

import java.util
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取订单数据
    val resource = getClass.getResource("/OrderLog.csv")
    val csvData = env.readTextFile(resource.getPath)
    val socketData = env.socketTextStream("localhost", 7777)
    val DataStream = socketData.map(data => {
      val dataArray = data.split(",")
      OrderEvent2(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
      .assignAscendingTimestamps(_.eventTime * 1000L)

    // 3. transform 处理数据
    val orderEventStream = DataStream.keyBy(_.orderId)

    // 4.CEP
    // 定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent2]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))
    //前后的订单在15分钟内
    // 把模式应用到stream上，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)
    // 调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOutputTag = new OutputTag[OrderResult1]("orderTimeout")
    //定义一个标签
    val resultStream = patternStream.select(
      orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect()
    ) //另外颗粒化的写法是这样写的 select(..)(..)(..)。

    resultStream.print("支付成功的订单")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("超时的订单")
    env.execute("order timeout job")
  }
}

// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent2, OrderResult1] {
  //PatternTimeoutFunction<IN, OUT>
  override def timeout(map: util.Map[String, util.List[OrderEvent2]], l: Long): OrderResult1 = { //Map[k,v]，k是匹配模式的名字，v是匹配成功的事件。l表示接收不匹配的事件的最后一条数据的时间戳,当然最后一条数据与匹配成功的差距是最小的。如果没有最后一条(即没有第2条数据,或3,4...)，一直没有收到的话就，使用默认的虚假时间9223372036854775807ms，表示接受到在未来的很久很久时间点。
    val timeoutOrderId = map.get("begin").iterator().next().orderId //注意 这里是没有get("follow")的。
  val ite = map.get("begin").iterator() //注意 这里是没有get("follow")的。
    OrderResult1(timeoutOrderId, "超时时间：" + l + "ms") //1，表示接收符合延迟的事件的最后一条数据的时间点，如果接受到的数据，这后面一直没有匹配到，则表示最后一条数据匹配到的时间为1558431951000，即虚假时间(未来的时间点)。如果匹配到了，则就表示最后一条数据的时间
  }
}

// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent2, OrderResult1] {
  // PatternSelectFunction<IN, OUT>
  override def select(map: util.Map[String, util.List[OrderEvent2]]): OrderResult1 = {
    val payedOrderId = map.get("follow").iterator().next().orderId
    val time = map.get("follow").iterator().next().orderId
    OrderResult1(payedOrderId, "payed successfully") //self 如果就来了一条的9223372036854775807ms
  }
}
