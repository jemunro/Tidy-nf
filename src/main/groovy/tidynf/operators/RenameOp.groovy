package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.helpers.TidyChecks.checkContains
import static tidynf.helpers.TidyChecks.checkContainsNot
import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch

class RenameOp {
    private String methodName = 'rename'
    private DataflowChannel source
    private String newKey
    private String oldKey
    private LinkedHashSet keySet

    RenameOp(DataflowChannel source, String newKey, String oldKey) {

        this.source = source
        this.newKey = newKey
        this.oldKey = oldKey
    }

    DataflowChannel apply() {

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

            data.collectEntries { k, v -> [(oldKey == k ? newKey: k): v] }
        }
    }

    void firstChecks() {
        checkContains(keySet, oldKey, methodName)
        checkContainsNot(keySet, newKey, methodName)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
    }
}