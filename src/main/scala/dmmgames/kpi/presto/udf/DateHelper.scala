package dmmgames.kpi.presto.udf

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
  * Created by noguchi-yuya on 2018/10/24.
  */
object DateHelper {

  val unixTimeFormat = "yyyy-MM-dd HH:mm:ss"
  val dateFormat = "yyyy-MM-dd"

  def dateFormatPattern(date: String): Boolean =  {
    try {
      LocalDateTime.parse(date,DateTimeFormatter.ofPattern(unixTimeFormat))
      true
    } catch {
      case e: java.time.format.DateTimeParseException => {
        false
      }
    }
  }
}
