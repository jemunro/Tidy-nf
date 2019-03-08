package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecker.checkHasKey
import static tidynf.TidyChecker.checkIsType
import static tidynf.TidyChecker.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys

class PullOp {

    private String method_name
    private DataflowChannel source
    private String key

    PullOp(String method_name, DataflowChannel source, String key) {

        this.method_name = method_name
        this.source = source
        this.key = key
    }

    DataflowChannel apply() {

        withKeys(source).map {

            runChecks(it)

            it.data[key]
        }
    }

    void runChecks(LinkedHashMap map) {
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, map.data.keySet() as List, method_name)
        checkHasKey(map.data, key, method_name)
    }
}