package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.helpers.TidyChecks.checkContains
import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch

class PullOp {

    private String methodName = 'pull'
    private DataflowChannel source
    private String key
    private LinkedHashSet keySet

    PullOp(DataflowChannel source, String key) {

        this.source = source
        this.key = key
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

            data[key]
        }
    }

    void firstChecks() {
        checkContains(keySet, key, methodName)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
    }
}