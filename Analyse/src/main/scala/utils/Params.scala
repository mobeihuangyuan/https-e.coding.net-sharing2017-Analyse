package utils

/**
 * Created by laishun on 15/10/12.
 */

case class  Params(
                    isOnline: Boolean = false, //参数决定数据是否上线
                    deleteOld: Boolean = false, // 是否删除旧数据
                    isSave: Boolean = false,  //是否保存结果
                    isEvaluate: Boolean = false,  //是否评估模型
                    startDate: String = null,
                    startHour: String = null,
                    endDate: String = null,
                    debug: Boolean = false, //打印调试信息
                    mode: String = null, // 数据更新逻辑
                    algType: String = null,
                    paramMap: Map[String, String] = null //其他非命令行参数
                  )



