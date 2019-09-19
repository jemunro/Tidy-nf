package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.*
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList
import static tidynf.exception.TidyError.error

class MutateOp {

    private String method_name
    private DataflowChannel source
    private Binding with
    private Closure dehydrated

    MutateOp(String method_name, DataflowChannel source, Closure closure, Map with){

        this.method_name = method_name
        this.source = source
        this.with = with as Binding
        this.dehydrated = closure.dehydrate()

    }

    DataflowChannel apply() {
        withKeys(source).map {

            runChecks(it)

            def data = new Binding(it.data as LinkedHashMap)
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
