package hotitems_analysis


import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {
  def main(args: Array[String]): Unit = {
    //    1. 创建执行环境和时间语义
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //    2. 读取数据
    val dataStream = env.readTextFile("...behavior")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)


    /** *
      * 具体处理过程
      */
    //    2.1 过滤，只要含pv的
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult()) //窗口聚合
      //    按窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    //    3. sink
    processedStream.print()

    env.execute("hot items job")
  }
}

//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc1 + acc
}

//自定义预聚合函数计算时间戳平均数
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  //  初始值
  override def createAccumulator(): (Long, Int) = (0L, 0)

  //  来一个数据做累加操作
  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc1._2 + acc._2)
}

// 自定义 窗口函数，输出itemViewResult
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//自定义处理函数，获取top N
class TopNHotItems(topN: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //    把每条数据存入状态列表
    itemState.add(value)

    //    注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  // 定时器触发时，所有数据排序并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将所有state中的数据取出，放到list buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    //    便利items
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }

    //    按照count大小排序，降序排序，并取前N个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    //    清空状态
    itemState.clear()

    //    将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("Time: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      result.append("No.").append(i + 1).append(":")
        .append(" 商品Id=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }

    result.append("==========================")
    //    控制输出频率
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}