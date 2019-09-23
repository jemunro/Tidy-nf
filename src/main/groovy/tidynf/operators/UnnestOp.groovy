package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkContainsAll
import static tidynf.TidyChecks.checkEqualSizes
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch

class UnnestOp {

    private String method_name = 'unnest'
    private DataflowChannel source
    private List at
    private LinkedHashSet keySet


    UnnestOp(DataflowChannel source, List at){

        this.source = source
        this.at = at
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

            def this_at = at
            if (! this_at){
                this_at = data.findAll { k, v -> v instanceof List }.collect { it.key }
            }
            if (! this_at) {
                [ data ]
            } else {
                checkEqualSizes(this_at.collect { k -> data[k] }, method_name)
                def n = data[this_at[0]].size()
                (0..<n).collect { i ->
                    data.collectEntries { k, v ->
                        [(k): this_at.contains(k) ? data[k][i] : data[k]]
                    }
                }
            }
        }.flatMap { it }
    }

    void firstChecks() {
        checkContainsAll(keySet, at, method_name)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }
}