package dmmgames.kpi.presto.udf

/**
  * Created by noguchi-yuya on 2018/10/23.
  */
import java.util.concurrent.TimeUnit.{DAYS,MILLISECONDS}
import org.joda.time.chrono.ISOChronology
import java.lang.Math.toIntExact

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneId

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlType}

import io.airlift.slice.Slice

object DateAddUDF {

  val chro = ISOChronology.getInstanceUTC()
  val unixTimeFormat = "yyyy-MM-dd HH:mm:ss"
  val dateFormat = "yyyy-MM-dd"

  @Description("date_add(<<date1> , <<long1>>)")
  @ScalarFunction("date_add")
  @SqlType(StandardTypes.DATE)
  def impalaFuncDateAdd(@SqlType(StandardTypes.DATE) date: Long,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    val millis = chro.dayOfMonth().add(DAYS.toMillis(date), toIntExact(addDay))
    MILLISECONDS.toDays(millis)
  }

  @Description("date_add(<<varchar1>> , <<long1>>)")
  @ScalarFunction("date_add")
  @SqlType(StandardTypes.TIMESTAMP)
  def impalaFuncDateAddFieldToString(@SqlType(StandardTypes.VARCHAR) date: Slice,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    val localTimeUnix = if (dateFormatPattern(date.toStringUtf8)) {
      LocalDateTime.parse(date.toStringUtf8,DateTimeFormatter.ofPattern(unixTimeFormat)).plusDays(addDay).atZone(ZoneId.systemDefault).toEpochSecond
    } else {
      LocalDate.parse(date.toStringUtf8,DateTimeFormatter.ofPattern(dateFormat)).plusDays(addDay).atStartOfDay(ZoneId.systemDefault).toEpochSecond
    }
    MILLISECONDS.toMicros(localTimeUnix)
  }

//  @Description("date_add(<<varchar1>> , <<long1>>)")
//  @ScalarFunction("date_add")
//  @SqlType(StandardTypes.DATE)
//  def impalaFuncDateAddFieldToStringForDate(@SqlType(StandardTypes.VARCHAR) date: Slice,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
//    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd")
//    val localTimeUnix = LocalDate.parse(date.toStringUtf8,format).atStartOfDay(ZoneId.systemDefault).toEpochSecond
//    val millis = chro.dayOfMonth().add(MILLISECONDS.toMicros(localTimeUnix), toIntExact(addDay))
//    MILLISECONDS.toDays(millis)
//  }

  private
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
