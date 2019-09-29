package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.helpers.Checks.checkIsType
import static tidynf.helpers.Checks.checkKeysMatch

class CollectColsOp {

    private String methodName = 'collect_cols'
    private DataflowQueue source
    private LinkedHashSet keySet

    CollectColsOp(DataflowQueue source) {
        this.source = source
    }

    DataflowVariable apply() {

        source.map {

            checkIsType(it, LinkedHashMap, methodName)
            def data = it as LinkedHashMap

            synchronized (this) {
                if (! keySet) {
                    keySet = data.keySet()
                }
            }

            mapChecks(data)

            data

        }.toList().map {

            keySet?.collectEntries{ k -> [ (k): it.collect { it[k] } ] }
        }
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
    }
}