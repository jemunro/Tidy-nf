package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch

class UnnameOp {

    private String methodName = 'unname'
    private DataflowChannel source
    private LinkedHashSet keySet

    UnnameOp(DataflowChannel source) {

        this.source = source
    }

    DataflowChannel apply() {

        source.map {

            checkIsType(it, LinkedHashMap, methodName)
            def data = it as LinkedHashMap

            synchronized (this) {
                if (! keySet) {
                    keySet = data.keySet()
                }
            }

            mapChecks(data)

            data.values() as ArrayList
        }
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
    }
}