package tidynf.operators

import groovyx.gpars.dataflow.DataflowWriteChannel

import static tidynf.TidyChecks.checkEqualSizes
import static tidynf.TidyChecks.checkHasKeys
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList

class UnnestOp {

    private String method_name
    private DataflowWriteChannel source
    private List at


    UnnestOp(String method_name, DataflowWriteChannel source, List at){

        this.method_name = method_name
        this.source = source
        this.at = at
    }

    DataflowWriteChannel apply() {

        withKeys(source).map {

            runChecks(it)

            def data = it.data as LinkedHashMap
            def _at= at
            if (! _at){
                _at = data.findAll { k, v -> v instanceof List }.collect { it.key }
            }
            if (! _at) {
                [ it.data ]
            } else {
                checkEqualSizes(_at.collect { k -> data[k] }, method_name)
                def n = data[_at[0]].size()
                (0..<n).collect { i ->
                    data.collectEntries { k, v ->
                        [(k): _at.contains(k) ? data[k][i] : data[k]]
                    }
                }
            }
        }.flatMap { it }
    }

    void runChecks(LinkedHashMap map) {
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        //checkKeysMatch(map.keys, keySetList(map.data), method_name)
        checkHasKeys(map.data, at, method_name)
    }
}