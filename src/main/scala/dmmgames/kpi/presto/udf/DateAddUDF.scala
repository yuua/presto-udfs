package dmmgames.kpi.presto.udf

/**
  * Created by noguchi-yuya on 2018/10/23.
  */
import java.util.concurrent.TimeUnit.{DAYS,MILLISECONDS}
import org.joda.time.chrono.ISOChronology
import org.joda.time.DateTimeZone
import java.lang.Math.toIntExact

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneId

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlType}

import io.airlift.slice.Slice

object DateAddUDF {

  val chronology = ISOChronology.getInstance(DateTimeZone.forID("Asia/Tokyo"))

  @Description("date_add(<<date1> , <<long1>>)")
  @ScalarFunction("date_add")
  @SqlType(StandardTypes.TIMESTAMP)
  def impalaFuncDateAdd(@SqlType(StandardTypes.DATE) date: Long,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    chronology.dayOfMonth().add(DAYS.toMillis(date) - 32400000, toIntExact(addDay))
  }

  @Description("date_add(<<timestamp1> , <<long1>>)")
  @ScalarFunction("date_add")
  @SqlType(StandardTypes.TIMESTAMP)
  def impalaFuncUnixTimeAdd(@SqlType(StandardTypes.TIMESTAMP) date: Long,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    chronology.dayOfMonth().add(date, toIntExact(addDay))
  }

  @Description("date_add(<<varchar1>> , <<long1>>)")
  @ScalarFunction("date_add")
  @SqlType(StandardTypes.TIMESTAMP)
  def impalaFuncDateAddFieldToString(@SqlType(StandardTypes.VARCHAR) date: Slice,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    val localTimeUnix = if (DateHelper.dateFormatPattern(date.toStringUtf8)) {
      LocalDateTime.parse(date.toStringUtf8,DateTimeFormatter.ofPattern(DateHelper.unixTimeFormat)).plusDays(addDay).atZone(ZoneId.systemDefault).toEpochSecond
    } else {
      LocalDate.parse(date.toStringUtf8,DateTimeFormatter.ofPattern(DateHelper.dateFormat)).plusDays(addDay).atStartOfDay(ZoneId.systemDefault).toEpochSecond
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

}
