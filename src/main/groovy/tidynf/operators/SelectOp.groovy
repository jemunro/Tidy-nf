package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkContainsAll
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyChecks.checkNonEmpty

class SelectOp {

    private String method_name = 'select'
    private DataflowChannel source
    private List keys
    private LinkedHashSet keySet

    SelectOp(DataflowChannel source, List keys){

        this.source = source
        this.keys = keys
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

            data.subMap(keys)
        }
    }

    void firstChecks() {
        checkNonEmpty(keys, method_name)
        checkContainsAll(keySet, keys, method_name)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }
}