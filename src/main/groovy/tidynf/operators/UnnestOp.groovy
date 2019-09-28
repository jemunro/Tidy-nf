package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.helpers.TidyChecks.checkContainsAll
import static tidynf.helpers.TidyChecks.checkEqualSizes
import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch

class UnnestOp {

    private String methodName = 'unnest'
    private DataflowChannel source
    private List at
    private LinkedHashSet keySet


    UnnestOp(DataflowChannel source, List at){

        this.source = source
        this.at = at
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

            def this_at = at
            if (! this_at){
                this_at = data.findAll { k, v -> v instanceof List }.collect { it.key }
            }
            if (! this_at) {
                [ data ]
            } else {
                checkEqualSizes(this_at.collect { k -> data[k] }, methodName)
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
        checkContainsAll(keySet, at, methodName)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
    }
}