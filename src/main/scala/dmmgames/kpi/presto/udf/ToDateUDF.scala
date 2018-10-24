/**
  * Created by noguchi-yuya on 2018/10/12.
  */

package dmmgames.kpi.presto.udf

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit._

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlNullable, SqlType}
import io.airlift.slice.Slice
import io.airlift.slice.Slices.utf8Slice

object ToDateUDF {

  @Description("to_date(<<timestamp1>>)")
  @ScalarFunction("to_date")
  @SqlType(StandardTypes.VARCHAR)
  def toDate(@SqlType(StandardTypes.TIMESTAMP) date: Long): Slice = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    utf8Slice(dateFormat.format(date))
  }

  @Description("to_date(<<varchar1>>)")
  @ScalarFunction("to_date")
  @SqlType(StandardTypes.VARCHAR)
  def toDate(@SqlType(StandardTypes.VARCHAR) date: Slice): Slice = {
    val localTimeUnix = if (DateHelper.dateFormatPattern(date.toStringUtf8)) {
      LocalDateTime.parse(date.toStringUtf8,DateTimeFormatter.ofPattern(DateHelper.unixTimeFormat)).atZone(ZoneId.systemDefault).toEpochSecond
    } else {
      return date
    }
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    utf8Slice(dateFormat.format(MILLISECONDS.toMicros(localTimeUnix)))
  }
}
