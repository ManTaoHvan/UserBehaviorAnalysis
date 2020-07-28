package com.study.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取数据
    // 读取端口的数据
    //     val socket= env.socketTextStream("localhost", 7777)
    // 读取文件数据
    val resource = getClass.getResource("/LoginLog.csv")
    val csvdata = env.readTextFile(resource.getPath) //如果我们读取的是有限数据的文件，那么就要注意，在远行程序的时候，会生成为什么水位线之前的数据怎么也计算了且输出到控制台？答：因为数据已经被程序读完了，程序知道不会在有数据了过来了，水位线之前的数据也就不在延迟处理了，因为在也没有数据，也就谈不上再次延迟了。

    val socketData = csvdata.map(data => {
      val dataArray = data.split(",")
      LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      }) //注意 cep

    // 3. transform 处理数据
    val loginEventStream = socketData.keyBy(_.userId)

    // 4.获取模式流
    // 定义匹配模式，即类似正则匹配表达式，这里我们使用严格近邻
    val loginFailPattern = Pattern
      .begin[LoginEvent]("begin event").where(_.eventType == "fail")
      .next("middle event").where(_.eventType == "fail")
      .next("end event").where(_.eventType == "fail") //当然这里还可以继续next下去
      .within(Time.seconds(2))
    //要求“前-中-后”事件的之间的时间在相隔之内2秒，那就是恶意登录    val loginFailPattern = Pattern
    // 在事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)
    // 从pattern stream上应用select function，检出匹配事件序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())
    loginFailDataStream.print()
    env.execute("login fail with cep job")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中按照名称取出对应的事件
    //  val iter = map.get("begin").iterator()
    val bieginFail = map.get("begin event").iterator().next()
    val endFail = map.get("end event").iterator().next()
    Warning(bieginFail.userId, bieginFail.eventTime, endFail.eventTime, "：该用户在该时间段内出现恶意登录！")
  }
}
