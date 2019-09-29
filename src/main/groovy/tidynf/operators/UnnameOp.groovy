package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidynf.exception.IllegalTypeException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.isType

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