package tidynf.dataframe

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowVariable
import tidynf.operators.SelectOp

class DataflowDataFrame implements AbstractDataFrame {

    private DataflowChannel source
    private boolean isVariable

    DataflowDataFrame(DataflowChannel source) {
        this.source = source
        this.isVariable = source instanceof DataflowVariable
    }

    DataflowChannel as_channel() {
        this.source
    }

    DataflowDataFrame select(Collection vars){
        new SelectOp(this.source, vars as List).apply() as DataflowDataFrame
    }

    DataflowDataFrame mutate(Closure cl){
       this.source.map { (it as ListOfMapDataFrame).mutate(cl) }
    }
}
