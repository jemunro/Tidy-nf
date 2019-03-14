package tidynf.operators


import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.TidyChecks.checkHasKeys
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList

class ToGroupSizeOp {

    private String method_name
    private DataflowQueue source
    private List by

    ToGroupSizeOp(String method_name, DataflowQueue source, List by){

        this.method_name = method_name
        this.source = source
        this.by = by

    }

    DataflowVariable apply() {

        withKeys(source)
            .map {
                runChecks(it)
                [ by.collect { k -> it.data[k] }, null ] }
            .groupTuple(by:0)
            .map {
                [ by, it[0] ]
                    .transpose()
                    .collectEntries { k, v -> [ (k) : v ] }
                    .with { m -> m + [size: it[1].size() ] } }
            .toList()
            .map {
                keySetList(it[0])
                    .collectEntries{ k -> [ (k): it.collect { it[k] } ] } }
    }


    void runChecks(LinkedHashMap map) {

        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, keySetList(map.data), method_name)
        checkHasKeys(map.data, by, method_name)
    }

}