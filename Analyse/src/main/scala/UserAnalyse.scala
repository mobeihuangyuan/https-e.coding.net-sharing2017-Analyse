import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.slf4j.LoggerFactory
import com.alibaba.fastjson.JSON
import com.hadoop.mapreduce.LzoTextInputFormat
import com.sharing.utils.{BaseClass, ParamsParseUtil}
import common.{TimeDate, mysql_paras}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import utils.Params

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object UserAnalyse extends BaseClass{

  var point_ratio=0.2
  val logger=LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    logger.info("Begin init spark context ...")
    init()
    logger.info("End init spark context ...")
    logger.info("Begin execute ...")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val params = p
        logger.info("输入参数信息: " + params)
        execute(params = params)
      }
      case _ => {}
    }
    logger.info("Destroy spark context ...")
    destroy()
  }

  override def execute(params: Params): Unit = {
    if(!params.paramMap.contains(mysql_paras.userAnalyse_active_ratio)) {
      println("args into error ! Please input only a num!")
      return
    }
     var ratio= TimeDate.show(params.paramMap.get(mysql_paras.userAnalyse_active_ratio)).toDouble
     point_ratio=ratio
     println(" params.paramMap=="+params.paramMap)
     println("ratio=="+ratio+"  point_ratio="+point_ratio)
     val result_array=getActiveDecreseUser(ratio).toList

     writeMysql(result_array)
  }



  def getActiveDecreseUser(ratio_percent:Double)={

    val sql="select a.user_id,a.count as count1,b.count as count2 from (select user_id,count(*) as count from "+mysql_paras.tablename_dw_mysql_user_orders+" where day>="+TimeDate.getThisMonth_firstday_yyyyMMdd+" group  by  user_id order by user_id) as a left outer join (select user_id,count(*) as count from "+mysql_paras.tablename_dw_mysql_user_orders+"  where day>="+TimeDate.getLastMonth_firstday_yyyyMMdd+" and day<"+TimeDate.getThisMonth_firstday_yyyyMMdd+"    group  by  user_id order by user_id) as b where a.user_id=b.user_id"
    println(sql)
    sqlContext.sql(sql).toDF("user_id","count_in","count_out").registerTempTable("analyse_log")
    import java.text.DecimalFormat
    val format = new DecimalFormat("0.0000")
    val result_array=sqlContext.sql("select * from analyse_log").rdd.map(x=>(x.getLong(0),format.format(x.getLong(1).toDouble/x.getLong(2)))).filter(x=>x._2.toDouble<ratio_percent).sortBy(_._2)
                      .map(x=>(x._1+","+x._2)).collect()
    result_array.take(10).foreach(println)
    result_array
  }



  //写入mysql库
  def writeMysql(list:List[String]): Unit ={
    val hostname = mysql_paras.hostname
    val username = mysql_paras.username
    val password = mysql_paras.password
    val url =mysql_paras.url
    val dbc = mysql_paras.dbc
    val tablename=mysql_paras.activeRate_tablename
    println("mysql url=="+url)
    println(" before mysql point_ratio="+point_ratio)
    var conn = DriverManager.getConnection(url, username, password)
    var sql= "insert into  " + tablename + "("+mysql_paras.column_day+","+mysql_paras.columnname_user_id+","+mysql_paras.column_launcher_active_ratio+","+mysql_paras.column_launcher_point_ratio+","+mysql_paras.column_create_time+") values (?,?,?,?,?) "
    println("insert sql="+sql)
    var ps=conn.prepareStatement(sql)
    for(data<- list){
      var str=data.split(",")
      if(str(1).toDouble<point_ratio){
          ps.setString(1,TimeDate.getToday_yyyyMMdd())
          ps.setLong(2, str(0).toLong)
          ps.setDouble(3, str(1).toDouble)
          ps.setDouble(4, point_ratio)
          ps.setLong(5,System.currentTimeMillis()/1000)
          ps.executeUpdate()
      }
    }
    ps.close()
    conn.close()
  }





}
