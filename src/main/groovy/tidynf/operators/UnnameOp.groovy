package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList

class UnnameOp {

    private String method_name = 'unname'
    private DataflowChannel source
    private LinkedHashSet keySet

    UnnameOp(DataflowChannel source) {

        this.source = source
    }

    DataflowChannel apply() {

        source.map {

            checkIsType(it, LinkedHashMap, method_name)
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
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }
}