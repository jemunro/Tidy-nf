package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList

class ToRowsOp {

    private String method_name
    private DataflowQueue source

    ToRowsOp(String method_name, DataflowQueue source) {

        this.method_name = method_name
        this.source = source
    }

    DataflowVariable apply() {

        withKeys(source).map {

            runChecks(it)

            it.data

        }.toList()
    }

    void runChecks(LinkedHashMap map) {

        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, keySetList(map.data), method_name)
    }
}