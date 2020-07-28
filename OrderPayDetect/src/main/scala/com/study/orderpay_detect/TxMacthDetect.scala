package com.study.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 定义输入订单事件的样例类
case class OrderEvent2(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义接收流事件的样例类
case class ReceiptEvent2(txId: String, payChannel: String, eventTime: Long)

object TxMacthDetect {

  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取订单数据
    val Ordersocket = env.socketTextStream("localhost", 7777)
    val Orderlog = getClass.getResource("/OrderLog.csv")
    val Orderlogcsv = env.readTextFile(Orderlog.getPath)
    val OrderSocketsocketData = Ordersocket
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent2(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      }).assignAscendingTimestamps(_.eventTime * 1000L) //但是这个没有配合窗口使用,是没有用的，

    // 2. 读取支付到账事件流
    val receiptSocket = env.socketTextStream("localhost", 8888)
    val ResourceLog = getClass.getResource("/ReceiptLog.csv")
    val ResourceLogCsv = env.readTextFile(ResourceLog.getPath)
    val receiptSocketData = receiptSocket.map(data => {
      val dataArray = data.split(",")
      ReceiptEvent2(dataArray(0).trim, dataArray(1).trim, dataArray(2).toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)

    // 3. transform 处理数据
    val OrderDataStream = OrderSocketsocketData
      .filter(_.txId != "") //过滤没有的订单的数据
      .keyBy(_.txId)

    // 3. transform 处理数据
    val ResourceDataStream = receiptSocketData
      .keyBy(_.txId)

    // 4.将两条流连接起来，共同处理。(合并的流是同流不合污),注意 这里的流合并 ，定时器是是以最低水位线的流为标准，如果一个流的时间高于另一一个流的时间，者取时间最早的那个流
    val processedStream = OrderDataStream.connect(ResourceDataStream) // self 注意 如果是keyStream调用该方法的话，也是可以的，底层会自动把keyStream转成DataStream
      .process(new TxPayMatch())

    processedStream.print("匹配成功")
    processedStream.getSideOutput(new TxPayMatch().OrderLog_unmatchedPays).print("OrderLog匹配异常")
    processedStream.getSideOutput(new TxPayMatch().ReceiptLog_unmatchedReceipts).print("ReceiptLog匹配异常")

    env.execute("tx match job")
  }

}

class TxPayMatch() extends CoProcessFunction[OrderEvent2, ReceiptEvent2, (OrderEvent2, ReceiptEvent2)] { //CoProcessFunction<IN1, IN2, OUT>
  // 定义侧数据流tag
  val OrderLog_unmatchedPays = new OutputTag[OrderEvent2]("OrderLog_unmatchedPays")
  val ReceiptLog_unmatchedReceipts = new OutputTag[ReceiptEvent2]("ReceiptLog_unmatchedReceipts")

  // 定义状态来保存已经到达的订单支付事件和到账事件
  lazy val payState: ValueState[OrderEvent2] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent2]("pay-state", classOf[OrderEvent2]))
  lazy val receiptState: ValueState[ReceiptEvent2] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent2]("receipt-state", classOf[ReceiptEvent2]))

  // 订单支付事件数据的处理
  override def processElement1(pay: OrderEvent2, ctx: CoProcessFunction[OrderEvent2, ReceiptEvent2, (OrderEvent2, ReceiptEvent2)]#Context, out: Collector[(OrderEvent2, ReceiptEvent2)]): Unit = {
    // 判断有没有对应的到账事件
    val receipt = receiptState.value()
    if (receipt != null) {
      // 如果已经有receipt，在主流输出匹配信息，清空状态
      out.collect((pay, receipt))
      receiptState.clear()
    } else {
      // 如果还没到，那么把pay存入状态，并且注册一个定时器等待
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L) //等待5秒，等receipt来
    }
  }

  // 到账事件的处理
  override def processElement2(receipt: ReceiptEvent2, ctx: CoProcessFunction[OrderEvent2, ReceiptEvent2, (OrderEvent2, ReceiptEvent2)]#Context, out: Collector[(OrderEvent2, ReceiptEvent2)]): Unit = {
    // 同样的处理流程
    val pay = payState.value()
    if (pay != null) {
      out.collect((pay, receipt))
      payState.clear()
    } else {
      receiptState.update(receipt)
      ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent2, ReceiptEvent2, (OrderEvent2, ReceiptEvent2)]#OnTimerContext, out: Collector[(OrderEvent2, ReceiptEvent2)]): Unit = {
    // 到时间了，如果还没有收到某个事件，那么输出报警信息
    if (payState.value() != null) {
      // recipt没来，输出pay到侧输出流
      ctx.output(OrderLog_unmatchedPays, payState.value())
    }
    if (receiptState.value() != null) {
      ctx.output(ReceiptLog_unmatchedReceipts, receiptState.value())
    }
    payState.clear()
    receiptState.clear()
  }
}
