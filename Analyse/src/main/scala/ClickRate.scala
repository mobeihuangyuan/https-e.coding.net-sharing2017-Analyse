import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.slf4j.LoggerFactory
import com.alibaba.fastjson.JSON
import com.hadoop.mapreduce.LzoTextInputFormat
import com.sharing.utils.{BaseClass, ParamsParseUtil}
import common.mysql_paras
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import utils.Params

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object ClickRate extends BaseClass{

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
    val dayarray=common.TimeDate.getAlldays_InNdays_yyyy_mm_dd(1)
    println(dayarray)
    val numsday_array=Array(5,7)


    var resultarray=new ArrayBuffer[String]()
    for(day<- dayarray){
      var resultstr=""
      for(ndays<- numsday_array) {
        println("nowday =="+day+" ndays=="+ndays)
        val allFilePath = "hdfs://HDFS40435/user/hadoop/flume/as_rank/"+day

        val ndaysbefore_day=common.TimeDate.getPointDate_NdaysBefore_yyyy_MM_dd(day,ndays)
        val ndaysbefore_millis=common.TimeDate.parse_yyyyMMdd_to_millis(ndaysbefore_day.replace("-",""))
        getHdfsData(allFilePath,day,ndays,ndaysbefore_millis)

        //===================view data==================================
        val view_sql2="select distinct_id as user_id,properties.topic_id  as topic_id from  dw_logstash.events where length(distinct_id)<=8 and day="+day.replace("-","")+" and  event = 'ViewTopic'"
        val view_sql1 = "select distinct_id as user_id, properties.commodityid as topic_id from  dw_logstash.events where length(distinct_id)<=8 and day="+day.replace("-","")+" and  event = 'commodityDetail'"
        sqlContext.sql(view_sql1).union(sqlContext.sql(view_sql2)).toDF("user_id","topic_id").registerTempTable("view_log")

        //====================get in N days user_id========================
        val user_sql="select distinct(user_id) from dw_mysql.users where user_ctime>"+ndaysbefore_millis+" "
        sqlContext.sql(user_sql).toDF("user_id").registerTempTable("user_log")

        //====================get newbaoguang_log by filter user_id==============
        val baoguang_join_user_sql = "select t1.user_id, t1.topic_id,t1.request_time from baoguang_log as t1 join user_log as t2 on t1.user_id = t2.user_id"
        sqlContext.sql(baoguang_join_user_sql).toDF("user_id","topic_id","request_time").registerTempTable("newbaoguang_log")

        //====================get newview_log by filter user_id===================
        val baoguang_left_join_view_sql = "select t1.user_id,t1.topic_id, t2.user_id,t2.topic_id from newbaoguang_log as t1 left join view_log as t2 on t1.user_id = t2.user_id and t1.topic_id = t2.topic_id"
        sqlContext.sql(baoguang_left_join_view_sql).toDF("baoguang_uid","baoguang_tid","view_uid","view_tid").registerTempTable("view_baoguang_log")

        // =================get in Ndays ClickRate===========================
        val baoguangcount=sqlContext.sql("select * from newbaoguang_log where user_id is not null and topic_id is not null").count().toDouble
        val viewcount=sqlContext.sql("select * from view_baoguang_log where view_uid is not null and view_tid is not null").count()
        import java.text.DecimalFormat
        val format = new DecimalFormat("0.0000")
        val result_percent=format.format(viewcount/baoguangcount)

        // ==================get before Ndays ClickRate==================
        val viewcount_total=sqlContext.sql("select * from view_log where length(user_id)<=8 ").count
        val viewcount_before=viewcount_total-viewcount
        val baoguang_total=sqlContext.sql("select * from baoguang_log where length(user_id)<=8").count.toDouble
        val baoguangcount_before=baoguang_total-baoguangcount
        val result_percent_before=format.format(viewcount_before/baoguangcount_before)

        println("nowday =="+day+",in "+ndays +" clickRate="+result_percent+", out "+ndays+" clickRate= "+result_percent_before)

        resultarray+=day+","+ndays+"_in,"+result_percent
        resultarray+=day+","+ndays+"_out,"+result_percent_before
      }

    }
    println(resultarray)
    writeMysql(resultarray.toList)

    //day,user_create_type,launcher_click_ratio,create_time
//    val resultRDD=sc.makeRDD(resultarray.toList).map(x=>{
//      val str=x.split(",")
//      (str(0),str(1),str(2).toDouble,System.currentTimeMillis()/1000)
//    })
//    println("resultRDD.size=="+resultRDD.count())
//    resultRDD.foreachPartition(x=>RddToMysql(mysql_paras.clickRate_tablename,x))

  }


  //获取hdfs 曝光数据并注册到临时表中
  def getHdfsData(hdfsPath:String,day:String,ndays:Int,ndaysbefore_millis:String): Unit ={

    //=================base oneday  baoguang  data before userid filter===========================================

    val detailFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val daymillis=common.TimeDate.parse_yyyyMMdd_to_millis(day.replace("-","")).toLong
    val nextdaymillis=common.TimeDate.parse_yyyyMMdd_to_millis(common.TimeDate.getPointDate_NdaysAfter_yyyy_MM_dd(day,1).replace("-","")).toLong
    val requestRdd = sc.newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](
      hdfsPath
    ).map(r=>(r._2.toString)).filter(r=>{
      r.contains("query index recommend topic time detail")
    })

    val logReg = """(\{.*\})""".r
    val dateReg = """(((20[0-3][0-9]-(0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|[0-1][0-9]):[0-5][0-9]:[0-5][0-9])""".r
    val temprdd=requestRdd.map(r=> {
      val topics = ListBuffer[(Long, Long, Long)]()
      val logMatch = logReg.findAllIn(r).toList
      val timeMatch = dateReg.findAllIn(r).toList
      val logCal = Calendar.getInstance()

      if (logMatch.size != 0 && timeMatch.size != 0 && timeMatch(0).size != 0) {
        try {
          logCal.setTime(detailFormat.parse(timeMatch(0).trim))
          val logObj = JSON.parseObject(logMatch(0))
          val uid = logObj.getJSONObject("reqForm").getOrDefault("userId", 0.toString).toString.toLong
          val topicsArr = logObj.getJSONArray("respTopics")

          (0 until topicsArr.size()).foreach(i => {
            val topicObj = topicsArr.getJSONObject(i)
            val datatime = logCal.getTimeInMillis / 1000
            if (datatime > daymillis && datatime < nextdaymillis) {
              topics.+=((uid, topicObj.getOrDefault("tid", 0.toString).toString.toLong, datatime))
            }
          })
        } catch {
          case e: Exception => {

          }
        }
      }

      topics.toList
      //}).filter(r=>r.size!=0).flatMap(r=>r).toDF("user_id","topic_id","request_time").registerTempTable("baoguang_log")
    }).filter(r=>r.size!=0).flatMap(r=>r)

    val schema = StructType(
      StructField("user_id",LongType) ::
        StructField("topic_id",LongType) ::
        StructField("request_time",LongType)
        :: Nil
    )
    val hdfsrdd=temprdd.map(r=> Row.apply(r._1,r._2,r._3))
    sqlContext.createDataFrame(hdfsrdd,schema).registerTempTable("baoguang_log")
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
      ps.setLong(4,System.currentTimeMillis()/1000)
      ps.executeUpdate()
      ps.close()
    }
    conn.close()
  }



  //写入到mysql中
  def RddToMysql(tablename:String,iterator: Iterator[(String,String,Double,Long)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    var sql = ""
    sql = "insert into  " + tablename + "("+mysql_paras.column_day+","+mysql_paras.column_user_create_type+","+mysql_paras.column_launcher_click_ratio+","+mysql_paras.column_create_time+")  values (?, ?, ?,?) "
    println(" sql=="+sql)
    try {
      val hostname = mysql_paras.hostname
      val username = mysql_paras.username
      val password = mysql_paras.password
      val url =mysql_paras.url
      val dbc = mysql_paras.dbc
      println("mysql url=="+url)
      conn = DriverManager.getConnection(url, username, password)
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setString(2, data._2)
        ps.setDouble(3, data._3)
        ps.setLong(4, data._4)
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }


}
