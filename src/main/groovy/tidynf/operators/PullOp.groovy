package tidynf.operators

import groovyx.gpars.dataflow.DataflowWriteChannel

import static tidynf.TidyChecks.checkHasKey
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList

class PullOp {

    private String method_name
    private DataflowWriteChannel source
    private String key

    PullOp(String method_name, DataflowWriteChannel source, String key) {

        this.method_name = method_name
        this.source = source
        this.key = key
    }

    DataflowWriteChannel apply() {

        withKeys(source).map {

            runChecks(it)

            it.data[key]
        }
    }

    void runChecks(LinkedHashMap map) {
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, keySetList(map.data), method_name)
        checkHasKey(map.data, key, method_name)
    }
}