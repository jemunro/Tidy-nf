package tidyflow.dataframe

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.dataframe.operators.MutateOp
import static tidyflow.dataframe.DataFrame.new_df

class DataflowDataFrame { //implements AbstractDataFrame {

    private static Integer instanceCount = 0
    private DataflowChannel source
    private Integer instanceID

    DataflowDataFrame(DataflowChannel source){

        this.source = source
        this.instanceID = nextInstance()
    }

    static DataflowDataFrame new_df_df(DataflowChannel source, convert = true){
        if (convert)
            new DataflowDataFrame(source.map { new_df(it)} )
        else
            new DataflowDataFrame(source)
    }

    static synchronized Integer nextInstance() {
        ++instanceCount
    }
}
