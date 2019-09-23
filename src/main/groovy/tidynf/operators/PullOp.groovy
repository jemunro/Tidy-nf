package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkContains
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch

class PullOp {

    private String method_name = 'pull'
    private DataflowChannel source
    private String key
    private LinkedHashSet keySet

    PullOp(DataflowChannel source, String key) {

        this.source = source
        this.key = key
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

            data[key]
        }
    }

    void firstChecks() {
        checkContains(keySet, key, method_name)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }
}