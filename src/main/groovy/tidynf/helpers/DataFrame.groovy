package tidynf.helpers

import groovyx.gpars.dataflow.DataflowChannel
import tidynf.operators.SelectOp

interface DataFrame {

    DataFrame t()

    DataFrame mutate(Closure cl)

    DataFrame select(Collection c)

}