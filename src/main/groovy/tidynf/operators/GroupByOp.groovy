package tidynf.operators


import groovyx.gpars.dataflow.DataflowQueue

import static tidynf.helpers.TidyChecks.checkContainsAll
import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch
import static tidynf.helpers.TidyChecks.checkNonEmpty

class GroupByOp {

    private String methodName = 'group_by'
    private DataflowQueue source
    private ArrayList by
    private LinkedHashSet keySet

    GroupByOp(DataflowQueue source, List by){

        this.source = source
        this.by = by

    }

    DataflowQueue apply() {

        source.map {

            checkIsType(it, LinkedHashMap, methodName)
            def data = it as LinkedHashMap

            synchronized (this) {
                if (! keySet) {
                    keySet = data.keySet()
                    firstChecks()
                }
            }

            mapChecks(data)

            [ data.subMap(by), data ]

        }.groupTuple(by:0)
            .map {
                keySet.collectEntries { k ->
                    [ (k) : (by.contains(k) ? it[0][k] : it[1].collect { m -> m[k] } )]
                }
            }
    }

    void firstChecks() {
        checkNonEmpty(by, methodName)
        checkContainsAll(keySet, by, methodName)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
    }

}