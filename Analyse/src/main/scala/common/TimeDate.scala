package common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.io.{LongWritable, Text}
import com.hadoop.mapreduce.LzoTextInputFormat
import java.util.Calendar
import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer
import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TimeDate{


  def main(args: Array[String]): Unit = {
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")


    println(getToday_yyyyMMdd)

  }

  def  getAlldaysyyyyMMdd_by_startday_endday(yyyyMMdd_startday_str:String,yyyyMMdd_endday_str:String)={
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val startday=yyyyMMdd_startday_str.replaceAll("-","")
    val enday=yyyyMMdd_endday_str.replaceAll("-","")
    val dateFiled = Calendar.DAY_OF_MONTH
    var beginDate = dateFormat.parse(startday)
    val endDate = dateFormat.parse(enday)
    val calendar = Calendar.getInstance()
    calendar.setTime(beginDate)
    val dateArray: ArrayBuffer[String] = ArrayBuffer()
    while (beginDate.compareTo(endDate) <= 0) {
      dateArray += dateFormat.format(beginDate)
      calendar.add(dateFiled, 1)
      beginDate = calendar.getTime
    }
    dateArray
  }

  def  getAlldaysyyyy_MM_dd_by_startday_endday(yyyy_MM_dd_startday_str:String,yyyy_MM_dd_endday_str:String)={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startday=yyyy_MM_dd_startday_str
    val enday=yyyy_MM_dd_endday_str
    val dateFiled = Calendar.DAY_OF_MONTH
    var beginDate = dateFormat.parse(startday)
    val endDate = dateFormat.parse(enday)
    val calendar = Calendar.getInstance()
    calendar.setTime(beginDate)
    val dateArray: ArrayBuffer[String] = ArrayBuffer()
    while (beginDate.compareTo(endDate) <= 0) {
      dateArray += dateFormat.format(beginDate)
      calendar.add(dateFiled, 1)
      beginDate = calendar.getTime
    }
    dateArray
  }

  def getToday_yyyyMMdd() ={
    val today=getNdaysBefore_yyyy_MM_dd(0).replace("-","")
    today
  }

  //获取本月1号，20180601格式
  def  getThisMonth_firstday_yyyyMMdd()={
     var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMM")
     var cal:Calendar=Calendar.getInstance()
     var month=dateFormat.format(cal.getTime)
     month+"01"
  }
  //获取上月1号，20180501格式
  def  getLastMonth_firstday_yyyyMMdd()={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    var cal:Calendar=Calendar.getInstance()
    var month=dateFormat.format(cal.getTime)
    var day=""
    if(month.endsWith("01")){
      month=(month.substring(0,4).toLong-1)+"12"
    }else{
      month=(month.toLong-1).toString
    }
    month+"01"
  }

  //根据指定日期获取上月1好
  def  getLastMonth_firstday_yyyyMMdd_by_yyyyMMddstr(yyyyMMddstr:String)={
    var datestr=yyyyMMddstr.replaceAll("-","").replaceAll("/","")
    if(datestr.length>6) datestr=datestr.substring(0,6)
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    var cal:Calendar=Calendar.getInstance()
    cal.setTime(dateFormat.parse(datestr))
    var month=dateFormat.format(cal.getTime)
    var day=""
    if(month.endsWith("01")){
      month=(month.substring(0,4).toLong-1)+"12"
    }else{
      month=(month.toLong-1).toString
    }
    month+"01"
  }


  def getPointDate_NdaysBefore_yyyy_MM_dd(yyyy_MM_dd_str:String,n:Int):String={
    var startTime=yyyy_MM_dd_str
    if(startTime.length==8) startTime=startTime.substring(0,4)+"-"+startTime.substring(4,6)+"-"+startTime.substring(6,8)
    if(startTime.length!=10||(!startTime.contains("-"))) return ""
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.setTime(dateFormat.parse(startTime))
    cal.add(Calendar.DATE,-n)
    var ndaybefore=dateFormat.format(cal.getTime())
    ndaybefore
  }

  def getPointDate_NdaysAfter_yyyy_MM_dd(yyyy_MM_dd_str:String,n:Int):String={
    var startTime=yyyy_MM_dd_str
    if(startTime.length==8) startTime=startTime.substring(0,4)+"-"+startTime.substring(4,6)+"-"+startTime.substring(6,8)
    if(startTime.length!=10||(!startTime.contains("-"))) return ""
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.setTime(dateFormat.parse(startTime))
    cal.add(Calendar.DATE,+n)
    var ndaybefore=dateFormat.format(cal.getTime())
    ndaybefore
  }

  def getNdaysBefore_yyyy_MM_dd(n:Int):String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-n)
    var ndaybefore=dateFormat.format(cal.getTime())
    ndaybefore
  }

  def getAlldays_InNdays_yyyy_mm_dd(n:Int) ={
    var list=new ArrayBuffer[String]()
    var num=1;
    while(num<=n){
      list+=getNdaysBefore_yyyy_MM_dd(num)
      num=num+1
    }
    list.toArray
  }

  def getNdaysBefore_yyyyMMdd(n:Int):String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-n)
    var ndaybefore=dateFormat.format(cal.getTime())
    ndaybefore
  }
  def getNdaysAfter_yyyyMMdd(n:Int):String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,+n)
    var ndaybefore=dateFormat.format(cal.getTime())
    ndaybefore
  }

  //输入日期字符串，转成时间戳
  def parse_yyyyMMdd_to_millis(yyyyMMdd:String):String={
    import org.joda.time._
    import org.joda.time.format._
    val date=DateTime.parse(yyyyMMdd.replaceAll("-","").replaceAll("/",""),DateTimeFormat.forPattern("yyyyMMdd")).getMillis/1000
    date.toString
  }

  def getNdaysBefore_timestamp(n:Int):String={
    val ndaysYYYYMMDD=getNdaysBefore_yyyyMMdd(n)
    val millis=parse_yyyyMMdd_to_millis(ndaysYYYYMMDD)
    millis
  }
  //正则匹配数字字符,
  def isIntByRegex(numstr : String) = {
    val pattern = """^(\d+)$""".r
    val num_str=numstr.replace(".","")
    num_str match {
      case pattern(_*) => true
      case _ => false
    }
  }
  //模式匹配，获取Option[String]里面的值
  def show(x: Option[String]) = x match {
    case Some(s) => s
    case None => "null"
  }
}
