package tidyflow.dataframe.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.dataframe.DataflowDataFrame

abstract class Operator {

    protected String name
    protected Integer counter = 0
    protected Boolean debug = false
    protected Map par
    protected DataflowChannel source

    abstract DataflowChannel apply()

}