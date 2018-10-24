package dmmgames.kpi.presto.udf

/**
  * Created by noguchi-yuya on 2018/10/24.
  */

import java.util.concurrent.TimeUnit.{DAYS,MILLISECONDS}
import org.joda.time.chrono.ISOChronology
import java.lang.Math.toIntExact

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlType}

object DateSubUDF {

  @Description("date_sub(<<date1>> , <<long1>>)")
  @ScalarFunction("date_sub")
  @SqlType(StandardTypes.DATE)
  def impalaFuncDateSub(@SqlType(StandardTypes.DATE) date: Long,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    val chro = ISOChronology.getInstanceUTC()
    val millis = chro.dayOfMonth().add(DAYS.toMillis(date), toIntExact(-addDay))
    MILLISECONDS.toDays(millis)
  }

}
