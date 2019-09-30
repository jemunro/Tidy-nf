package tidynf.helpers

import groovyx.gpars.dataflow.DataflowChannel
import tidynf.operators.SelectOp

interface DataFrame {

    DataFrame t()

    DataFrame mutate(Closure closure)

    DataFrame select(Collection variables)

//    List pull(String variable)

//    DataFrame left_join(DataFrame dataFrame)
//
//    DataFrame semi_join(DataFrame dataFrame)
//
//    DataFrame inner_join(DataFrame dataFrame)
//
//    DataFrame full_join(DataFrame dataFrame)

}