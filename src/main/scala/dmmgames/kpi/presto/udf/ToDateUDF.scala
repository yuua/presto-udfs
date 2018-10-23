/**
  * Created by noguchi-yuya on 2018/10/12.
  */

package dmmgames.kpi.presto.udf

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlNullable, SqlType}
import io.airlift.slice.Slice
import io.airlift.slice.Slices.utf8Slice

object ToDateUDF {

  @Description("to_date(UDF impala function)")
  @ScalarFunction("to_date")
  @SqlType(StandardTypes.VARCHAR)
  def toDate(@SqlType(StandardTypes.TIMESTAMP) date: Long): Slice = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    utf8Slice(dateFormat.format(date))
  }
}
