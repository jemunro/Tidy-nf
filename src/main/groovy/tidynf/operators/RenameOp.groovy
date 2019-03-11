package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkHasKeys
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys

class RenameOp {
    private String method_name
    private DataflowChannel source
    private String new_key
    private String old_key

    RenameOp(String method_name, DataflowChannel source, String new_key, String old_key) {

        this.method_name = method_name
        this.source = source
        this.new_key = new_key
        this.old_key = old_key
    }

    DataflowChannel apply() {

        withKeys(source).map {

            runChecks(it)

            it.data.collectEntries { k, v -> [(old_key == k ? new_key: k): v] }
        }
    }

    void runChecks(LinkedHashMap map) {
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, map.data.keySet() as List, method_name)
        checkHasKeys(map.data, [new_key, old_key], method_name)
    }
}