package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.helpers.TidyChecks.checkContainsAll
import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch
import static tidynf.helpers.TidyChecks.checkNonEmpty

class SelectOp {

    private String methodName = 'select'
    private DataflowChannel source
    private List keys
    private LinkedHashSet keySet

    SelectOp(DataflowChannel source, List keys){

        this.source = source
        this.keys = keys
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

            data.subMap(keys)
        }
    }

    void firstChecks() {
        checkNonEmpty(keys, methodName)
        checkContainsAll(keySet, keys, methodName)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
    }
}