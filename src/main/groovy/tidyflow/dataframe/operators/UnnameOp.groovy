package tidyflow.dataframe.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.exception.IllegalTypeException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.isType

class UnnameOp {

    private String methodName = 'unname'
    private DataflowChannel source

    UnnameOp(DataflowChannel source) {

        this.source = source
    }

    DataflowChannel apply() {

        source.map {

            if (! isType(it, Map))
                throw new IllegalTypeException(errMsg(methodName, "Required Map type\n" +
                        "got ${it.getClass().simpleName} with value $it"))

            LinkedHashMap data = it as LinkedHashMap

            data.values() as ArrayList
        }
    }

}