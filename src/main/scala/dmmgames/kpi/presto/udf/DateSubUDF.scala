package dmmgames.kpi.presto.udf

/**
  * Created by noguchi-yuya on 2018/10/24.
  */

import java.util.concurrent.TimeUnit.{DAYS, MILLISECONDS}

import org.joda.time.chrono.ISOChronology
import java.lang.Math.toIntExact
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneId

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlType}
import io.airlift.slice.Slice
import org.joda.time.DateTimeZone

object DateSubUDF {

  val chronology = ISOChronology.getInstance(DateTimeZone.forID("Asia/Tokyo"))

  @Description("date_sub(<<date1>> , <<long1>>)")
  @ScalarFunction("date_sub")
  @SqlType(StandardTypes.TIMESTAMP)
  def impalaFuncDateSub(@SqlType(StandardTypes.DATE) date: Long,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    chronology.dayOfMonth().add(DAYS.toMillis(date) - 32400000 , toIntExact(-addDay))
  }

  @Description("date_sub(<<timestamp1> , <<long1>>)")
  @ScalarFunction("date_sub")
  @SqlType(StandardTypes.TIMESTAMP)
  def impalaFuncUnixTimeSub(@SqlType(StandardTypes.TIMESTAMP) date: Long,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    chronology.dayOfMonth().add(date, toIntExact(-addDay))
  }

  @Description("date_sub(<<varchar1>> , <<long1>>)")
  @ScalarFunction("date_sub")
  @SqlType(StandardTypes.TIMESTAMP)
  def impalaFuncDateSubFieldToString(@SqlType(StandardTypes.VARCHAR) date: Slice,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    val localTimeUnix = if (DateHelper.dateFormatPattern(date.toStringUtf8)) {
      LocalDateTime.parse(date.toStringUtf8,DateTimeFormatter.ofPattern(DateHelper.unixTimeFormat)).minusDays(addDay).atZone(ZoneId.systemDefault).toEpochSecond
    } else {
      LocalDate.parse(date.toStringUtf8,DateTimeFormatter.ofPattern(DateHelper.dateFormat)).minusDays(addDay).atStartOfDay(ZoneId.systemDefault).toEpochSecond
    }
    MILLISECONDS.toMicros(localTimeUnix)
  }
}
