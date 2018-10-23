/**
 * Created by noguchi-yuya on 2018/10/12.
 */

package dmmgames.kpi.presto.udf;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import dmmgames.kpi.presto.udf.ToDateUDF;

public class UdfPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ToDateUDF.class)
                .build();
    }
}
