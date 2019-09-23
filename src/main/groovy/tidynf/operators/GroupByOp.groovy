package tidynf.operators


import groovyx.gpars.dataflow.DataflowQueue

import static tidynf.TidyChecks.checkContainsAll
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyChecks.checkNonEmpty

class GroupByOp {

    private String method_name = 'group_by'
    private DataflowQueue source
    private ArrayList by
    private LinkedHashSet keySet

    GroupByOp(DataflowQueue source, List by){

        this.source = source
        this.by = by

    }

    DataflowQueue apply() {

        source.map {

            checkIsType(it, LinkedHashMap, method_name)
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
        checkNonEmpty(by, method_name)
        checkContainsAll(keySet, by, method_name)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }

}