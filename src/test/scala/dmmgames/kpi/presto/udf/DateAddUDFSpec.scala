package dmmgames.kpi.presto.udf

/**
  * Created by noguchi-yuya on 2018/10/24.
  */
import org.scalatest._
import io.airlift.slice.Slices._

class DateAddUDFSpec extends FlatSpec with DiagrammedAssertions {

  val dateAdd = dmmgames.kpi.presto.udf.DateAddUDF

  it should "2018-01-01の1日後は2018-01-02を返す" in {
    val date = dateAdd.impalaFuncDateAdd(17531,1)
    assert(date / 1000 == 1514732400)
  }

  it should "2018-01-01 10:00:00の1日後は2018-01-02 10:00:00を返す" in {
    val date = dateAdd.impalaFuncUnixTimeAdd(1514854800,1)
    assert(date == 1601254800)
  }

  it should "文字列2018-01-01 10:00:00を渡した場合unixtimeが変えること" in {
    val date : Long = dateAdd.impalaFuncDateAddFieldToString(utf8Slice("2018-01-01 10:00:00"), 1)
    assert(date / 1000 == 1514854800)
  }

  it should "文字列2018-01-01を渡した場合unixtimeが変えること" in {
    val date : Long = dateAdd.impalaFuncDateAddFieldToString(utf8Slice("2018-01-01"), 1)
    assert(date / 1000 == 1514818800)
  }
}
