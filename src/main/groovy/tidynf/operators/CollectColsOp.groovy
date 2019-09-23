package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch

class CollectColsOp {

    private String method_name = 'collect_cols'
    private DataflowQueue source
    private LinkedHashSet keySet

    CollectColsOp(DataflowQueue source) {
        this.source = source
    }

    DataflowVariable apply() {

        source.map {

            checkIsType(it, LinkedHashMap, method_name)
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
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }
}