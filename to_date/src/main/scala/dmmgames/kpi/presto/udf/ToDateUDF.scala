/**
  * Created by noguchi-yuya on 2018/10/12.
  */

package dmmgames.kpi.presto.udf

import java.text.SimpleDateFormat

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlNullable, SqlType}
import io.airlift.slice.Slice
import io.airlift.slice.Slices.utf8Slice

object ToDateUDF {

  @Description("to_date(UDF impala function)")
  @ScalarFunction("to_date")
  @SqlType(StandardTypes.DATE)
  def toDate(@SqlNullable @SqlType(StandardTypes.TIMESTAMP) date: Slice): Slice = {
    if (date == null || date.toStringUtf8.isEmpty) {
      utf8Slice("empty date")
    } else {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      utf8Slice(dateFormat.format(date))
    }
  }
}
