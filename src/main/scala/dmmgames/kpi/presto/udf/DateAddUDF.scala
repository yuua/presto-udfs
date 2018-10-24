package dmmgames.kpi.presto.udf

/**
  * Created by noguchi-yuya on 2018/10/23.
  */
import java.util.concurrent.TimeUnit.{DAYS,MILLISECONDS}
import org.joda.time.chrono.ISOChronology
import java.lang.Math.toIntExact
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneId

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlType}

import io.airlift.slice.Slice

object DateAddUDF {

  @Description("date_add(<<date1> , <<long1>>)")
  @ScalarFunction("date_add")
  @SqlType(StandardTypes.DATE)
  def impalaFuncDateAdd(@SqlType(StandardTypes.DATE) date: Long,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    val chro = ISOChronology.getInstanceUTC()
    val millis = chro.dayOfMonth().add(DAYS.toMillis(date), toIntExact(addDay))
    MILLISECONDS.toDays(millis)
  }

  @Description("date_add(UDF impala function)")
  @ScalarFunction("date_add")
  @SqlType(StandardTypes.TIMESTAMP)
  def impalaFuncDateAddFieldToString(@SqlType(StandardTypes.VARCHAR) date: Slice,@SqlType(StandardTypes.BIGINT) addDay: Long): Long = {
    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val localTime = LocalDateTime.parse(date.toStringUtf8,format)
    localTime.plusDays(1).atZone(ZoneId.systemDefault).toEpochSecond
  }


}
