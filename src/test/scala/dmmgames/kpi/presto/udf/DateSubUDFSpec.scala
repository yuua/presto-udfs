package dmmgames.kpi.presto.udf

/**
  * Created by noguchi-yuya on 2018/10/24.
  */

import io.airlift.slice.Slices._
import org.scalatest._

class DateSubUDFSpec extends FlatSpec with DiagrammedAssertions {

  val dateSub = dmmgames.kpi.presto.udf.DateSubUDF

  it should "2018-01-01の1日後は2018-12-31を返す" in {
    val date = dateSub.impalaFuncDateSub(17531,1)
    assert(date / 1000 == 1514559600)
  }

  it should "2018-01-01 10:00:00の1日後は2018-02-28 10:00:00を返す" in {
    val date = dateSub.impalaFuncUnixTimeSub(1514854800,1)
    assert(date == 1428454800)
  }

  it should "文字列2018-01-01 10:00:00を渡した場合unixtimeが変えること" in {
    val date : Long = dateSub.impalaFuncDateSubFieldToString(utf8Slice("2018-01-01 10:00:00"), 1)
    assert(date / 1000 == 1514682000)
  }

  it should "文字列2018-01-01を渡した場合unixtimeが変えること" in {
    val date : Long = dateSub.impalaFuncDateSubFieldToString(utf8Slice("2018-01-01"), 1)
    assert(date / 1000 == 1514646000)
  }
}
