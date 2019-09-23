package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList

class CollectRowsOp {

    private String method_name = 'collect_rows'
    private DataflowQueue source
    private LinkedHashSet keySet
    private boolean sort

    CollectRowsOp(DataflowQueue source, Boolean sort) {

        this.source = source
        this.sort = sort
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

        }.with {
            sort ? it.toSortedList() : it.toList()
        }
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }
}