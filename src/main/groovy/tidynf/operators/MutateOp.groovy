package tidynf.operators

import groovyx.gpars.dataflow.DataflowWriteChannel

import static tidynf.TidyChecks.*
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList
import static tidynf.exception.TidyError.error

class MutateOp {

    private String method_name
    private DataflowWriteChannel source
    private Binding with
    private Closure dehydrated

    MutateOp(String method_name, DataflowWriteChannel source, Closure closure, Map with){

        this.method_name = method_name
        this.source = source
        this.with = new Binding(with.clone() as Map)
        this.dehydrated = closure.dehydrate()

    }

    DataflowWriteChannel apply() {
        withKeys(source).map {

            runChecks(it)

            def data = new Binding(it.data.clone() as LinkedHashMap)
            def rehydrated = dehydrated.rehydrate(with, data, null)
            try {
                rehydrated.call()
            } catch(MissingPropertyException e) {
                error("Unknown variable \"${e.getProperty()}\", data: \"${it.data.toString()}\", with: \"${with.getVariables().toString()}\"",
                      'mutate')
            }

            data.getVariables() as LinkedHashMap
        }
    }

    void runChecks(LinkedHashMap map) {
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        //checkKeysMatch(map.keys, keySetList(map.data), method_name)
    }
}
