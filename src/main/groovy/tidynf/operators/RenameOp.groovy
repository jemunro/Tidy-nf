package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkContains
import static tidynf.TidyChecks.checkContainsNot
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch

class RenameOp {
    private String method_name = 'rename'
    private DataflowChannel source
    private String new_key
    private String old_key
    private LinkedHashSet keySet

    RenameOp(DataflowChannel source, String new_key, String old_key) {

        this.source = source
        this.new_key = new_key
        this.old_key = old_key
    }

    DataflowChannel apply() {

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

            data.collectEntries { k, v -> [(old_key == k ? new_key: k): v] }
        }
    }

    void firstChecks() {
        checkContains(keySet, old_key, method_name)
        checkContainsNot(keySet, new_key, method_name)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }
}