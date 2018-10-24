package dmmgames.kpi.presto.udf

/**
  * Created by noguchi-yuya on 2018/10/24.
  */

import org.scalatest._

class DateSubUDFSpec extends FlatSpec with DiagrammedAssertions {

  val dateSub = dmmgames.kpi.presto.udf.DateSubUDF

  it should "2018-01-01の1日後は2018-01-02を返す" in {
    val date = dateSub.impalaFuncDateSub(17531,1)
    assert(date == 17530)
  }

}
