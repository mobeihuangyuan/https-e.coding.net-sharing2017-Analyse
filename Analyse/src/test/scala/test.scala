import java.text.SimpleDateFormat
import java.util.Calendar
import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io._

import common.mysql_paras

import scala.collection.mutable.ArrayBuffer

object test {

  def main(args: Array[String]): Unit = {

     var map=Map[String,String]()
    map+="q"->"0"
    map+="c"->"0"
    println(map)

     var array=new ArrayBuffer[String]()
     array+="2018-06-05,5_in,0.0000"
     array+="2018-06-05,5_out,0.0000"
     array+="2018-06-05,5_out,0.0000"
    writeMysql(array.toList)



    System.setProperty("HADOOP_USER_NAME","root")
    val sparkConf=new SparkConf().setAppName("ClickRate").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.makeRDD(array).map(x=>{
      val str=x.split(",")
      (str(0),str(1),str(2),System.currentTimeMillis()/1000)
    }).foreach(println)

    //sc.makeRDD(array).foreach(println)


  }
  //写入mysql库
  def writeMysql(list:List[String]): Unit ={
    val hostname = mysql_paras.hostname
    val username = mysql_paras.username
    val password = mysql_paras.password
    val url =mysql_paras.url
    val dbc = mysql_paras.dbc
    val tablename=mysql_paras.clickRate_tablename
    println("mysql url=="+url)
    var conn = DriverManager.getConnection(url, username, password)
    for(data<- list){
      var str=data.split(",")
      var sql= "insert into  " + tablename + "("+mysql_paras.column_day+","+mysql_paras.column_user_create_type+","+mysql_paras.column_launcher_click_ratio+","+mysql_paras.column_create_time+") values (?,?,?,?) "
      println("insert sql="+sql)
      var ps=conn.prepareStatement(sql)
      ps.setString(1,str(0))
      ps.setString(2, str(1))
      ps.setDouble(3, str(2).toDouble)
      ps.setLong(4,str(3).toLong)
      ps.executeUpdate()
      ps.close()
    }
    conn.close()
  }

  def getPointDate_NdaysBefore_yyyy_MM_dd(yyyy_MM_dd_str:String,n:Int):String={
    if(yyyy_MM_dd_str.length!=10||(!yyyy_MM_dd_str.contains("-"))) return ""
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.setTime(dateFormat.parse(yyyy_MM_dd_str))
    cal.add(Calendar.DATE,-n)
    var ndaybefore=dateFormat.format(cal.getTime())
    ndaybefore
  }
}
