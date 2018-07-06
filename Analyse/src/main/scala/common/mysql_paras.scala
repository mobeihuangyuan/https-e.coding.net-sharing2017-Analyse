package common

object mysql_paras {
  final val hostname = "172.17.172.39"
  final val username = "sharing_bi"
  final val password = "29KpwAviQGvZ"
  final val url = "jdbc:mysql://" + hostname + ":3306/sharing_bi"
  final val dbc = "jdbc:mysql://" + hostname + ":3306/sharing_bi?user=" + username + "&password=" + password

  final val clickRate_tablename="launcher_click_ratio"
  final val activeRate_tablename="launcher_active_ratio"
  final val ReturnRate_tablename="launcher_return_ratio"
  final val column_day="day"
  final val column_user_create_type="user_create_type"
  final val column_launcher_click_ratio="launcher_click_ratio"
  final val column_launcher_active_ratio="launcher_active_ratio"
  final val column_launcher_return_ratio="launcher_return_ratio"
  final val column_launcher_point_ratio="launcher_point_ratio"
  final val column_create_time="create_time"

  final val userAnalyse_active_ratio="active_ratio"
  final val userAnalyse_return_ratio="return_ratio"
  final val tablename_dw_mysql_user_orders="dw_mysql.user_orders"
  final val columnname_user_id="user_id"

  final val param_name_startday="startday"
  final val param_name_endday="endday"
  final val param_name_month="month"

  //回头率不指定起始时间查询范围时，默认查询60天的数据
  final val param_returnrate_select60days_data=60

}
