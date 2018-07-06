package com.sharing.analyse

import java.sql.DriverManager

import com.sharing.{Params, ParamsParseUtil}
import com.sharing.analyse.UserAnalyse._
import com.sharing.utils.BaseClass
import common.{TimeDate, mysql_paras}
import org.slf4j.LoggerFactory

object ReturnRate  extends BaseClass {

  var point_ratio = 0.7
  var startday_millis=""
  var endday_millis=""
  val logger = LoggerFactory.getLogger(this.getClass)

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
    if (!params.paramMap.contains(mysql_paras.userAnalyse_return_ratio)) {
      println("args into error ! Please input only a num!")
      return
    }
    var ratio = TimeDate.show(params.paramMap.get(mysql_paras.userAnalyse_return_ratio)).toDouble
    point_ratio = ratio
    println(" params.paramMap==" + params.paramMap)
    println("ratio==" + ratio + "  point_ratio=" + point_ratio)
    val result_array = getReturnUser(ratio).toList
    if(params.paramMap.contains(mysql_paras.param_name_startday)&&params.paramMap.contains(mysql_paras.param_name_endday)) {
      var startday_input=TimeDate.show(params.paramMap.get(mysql_paras.param_name_startday))
      var endday_input=TimeDate.show(params.paramMap.get(mysql_paras.param_name_endday))
      println("startday_input=="+startday_input+"  endday_input="+endday_input)
      startday_input=startday_input.replaceAll("-","").replaceAll("/","")
      endday_input=endday_input.replaceAll("-","").replaceAll("/","")
      if(TimeDate.isIntByRegex(endday_input)&&TimeDate.isIntByRegex(startday_input)){
        startday_millis=TimeDate.parse_yyyyMMdd_to_millis(startday_input)
        endday_millis=TimeDate.parse_yyyyMMdd_to_millis(endday_input)
      }

    }else{
      startday_millis=TimeDate.parse_yyyyMMdd_to_millis(TimeDate.getNdaysBefore_yyyy_MM_dd(mysql_paras.param_returnrate_select60days_data).replaceAll("-",""))
      endday_millis=(System.currentTimeMillis()/1000).toString
    }


    writeMysql(result_array)
  }


  def getReturnUser(ratio_percent: Double) = {

    val base_sql = "select  topic_user_id as user_id ,count(*) as count from dw_mysql.user_orders where order_ctime>"+startday_millis+" and order_ctime<"+endday_millis+" group by topic_user_id,user_id "
    sqlContext.sql(base_sql).toDF("user_id", "count").registerTempTable("base_log")

    val total_sql = "select user_id,count(*) as count from base_log   group by user_id  "
    sqlContext.sql(total_sql).toDF("user_id", "count").registerTempTable("user_total_log")

    val manyrecord_sql = "select user_id,count(*) as count from base_log where   count>1  group by user_id  "
    sqlContext.sql(manyrecord_sql).toDF("user_id", "count").registerTempTable("user_manyrecord_log")


    val join_sql = "select a.user_id,a.count as totalcount,b.count as manycount from user_total_log as a left join (select * from  user_manyrecord_log where   count>1) as b  on a.user_id=b.user_id"
    sqlContext.sql(join_sql).toDF("user_id", "totalcount", "manycount").registerTempTable("join_log")

    import java.text.DecimalFormat
    val format = new DecimalFormat("0.0000")
    val result_array = sqlContext.sql("select * from join_log  where manycount is not null ").rdd.map(x => (x.getInt(0), format.format(x.getInt(2).toDouble / x.getInt(1)))).filter(x => x._2.toDouble > ratio_percent).sortBy(_._2, false)
      .map(x => (x._1 + "," + x._2)).collect()


    result_array.take(10).foreach(println)
    result_array
  }


  //写入mysql库
  def writeMysql(list: List[String]): Unit = {
    val hostname = mysql_paras.hostname
    val username = mysql_paras.username
    val password = mysql_paras.password
    val url = mysql_paras.url
    val dbc = mysql_paras.dbc
    val tablename = mysql_paras.ReturnRate_tablename
    println("mysql url==" + url)
    println(" before mysql point_ratio=" + point_ratio)
    val driver = "com.mysql.jdbc.Driver"
    Class.forName(driver)
    var connection = DriverManager.getConnection(url, username, password)
    if(connection.isClosed()){
      println("error connecting to the Database!");
    }
    var sql = "insert into  " + tablename + "(" + mysql_paras.column_day + "," + mysql_paras.columnname_user_id + "," + mysql_paras.column_launcher_return_ratio + "," + mysql_paras.column_launcher_point_ratio + "," +        mysql_paras.column_create_time + ") values (?,?,?,?,?) "
    println("insert sql=" + sql)
    var ps = connection.prepareStatement(sql)
    for (data <- list) {
      var str = data.split(",")
      if (str(1).toDouble < point_ratio) {
        ps.setString(1, TimeDate.getToday_yyyyMMdd())
        ps.setLong(2, str(0).toLong)
        ps.setDouble(3, str(1).toDouble)
        ps.setDouble(4, point_ratio)
        ps.setLong(5, System.currentTimeMillis() / 1000)
        ps.executeUpdate()
      }
    }
    ps.close()
    connection.close()
  }


}
