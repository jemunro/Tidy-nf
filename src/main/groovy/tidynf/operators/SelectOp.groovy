package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecker.checkHasKeys
import static tidynf.TidyChecker.checkIsType
import static tidynf.TidyChecker.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys

class SelectOp {

    private String method_name
    private DataflowChannel source
    private List keys

    SelectOp(String method_name, DataflowChannel source, List keys){

        this.method_name = method_name
        this.source = source
        this.keys = keys
    }

    DataflowChannel apply() {

        withKeys(source).map {

            runChecks(it)

            keys.collectEntries { k -> [(k): data[k]] }
        }
    }

    void runChecks(LinkedHashMap map) {
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, map.data.keySet() as List, method_name)
        checkHasKeys(map.data, keys, method_name)
    }
}