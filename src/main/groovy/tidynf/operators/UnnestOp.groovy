package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkEqualSizes
import static tidynf.TidyChecks.checkHasKeys
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch

class UnnestOp {

    private String method_name
    private DataflowChannel source
    private List at


    UnnestOp(String method_name, DataflowChannel source, List at){

        this.method_name = method_name
        this.source = source
        this.at = at
    }

    DataflowChannel apply() {

        withKeys(source).map {

            runChecks(it)

            def data = it.data as LinkedHashMap

            if (! at){
                at = data.findAll { k, v -> v instanceof List }.collect { it.key }
            }
            if (! at) {
                [ it.data ]
            } else {
                checkEqualSizes(at.collect { k -> data[k] }, method_name)
                def n = data[at[0]].size()
                (0..<n).collect { i ->
                    data.collectEntries { k, v ->
                        [(k): at.contains(k) ? data[k][i] : data[k]]
                    }
                }
            }
        }.flatMap { it }
    }

    void runChecks(LinkedHashMap map) {

        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, map.data.keySet() as List, method_name)
        checkHasKeys(map.data, at, method_name)
    }
}