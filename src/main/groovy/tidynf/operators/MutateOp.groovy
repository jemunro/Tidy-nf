package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.*
import static tidynf.TidyDataFlow.withKeys

class MutateOp {

    private String method_name
    private DataflowChannel source
    private LinkedHashMap parent_data
    private Closure dehydrated

    MutateOp(String method_name, DataflowChannel source, Closure closure){

        this.method_name = method_name
        this.source = source
        this.parent_data = closure.binding.getVariables() as LinkedHashMap
        this.dehydrated = closure.dehydrate()

    }

    DataflowChannel apply() {
        withKeys(source).map {

            runChecks(it)

            def data = it.data as LinkedHashMap

            def binding = new Binding()
            def all_data = parent_data + data
            def rehydrated = dehydrated.rehydrate(all_data, binding, binding)
            rehydrated.call()

            data + (binding.getVariables() as LinkedHashMap)
        }
    }

    void runChecks(LinkedHashMap map) {
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, map.data.keySet() as List, method_name)
    }
}