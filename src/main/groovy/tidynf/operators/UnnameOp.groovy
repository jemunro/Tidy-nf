package tidynf.operators

import groovyx.gpars.dataflow.DataflowWriteChannel

import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList

class UnnameOp {

    private String method_name
    private DataflowWriteChannel source

    UnnameOp(String method_name, DataflowWriteChannel source) {

        this.method_name = method_name
        this.source = source
    }

    DataflowWriteChannel apply() {

        withKeys(source).map {

            runChecks(it)

            it.data.values() as List
        }
    }

    void runChecks(LinkedHashMap map) {

        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, keySetList(map.data), method_name)
    }
}