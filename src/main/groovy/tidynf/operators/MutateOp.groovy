package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyChecks.checkIsType
import static tidynf.exception.TidyError.tidyError

class MutateOp {

    private String method_name = 'mutate'
    private DataflowChannel source
    private Binding with
    private Closure dehydrated
    private LinkedHashSet keySet

    MutateOp(DataflowChannel source, Closure closure, Map with){

        this.source = source
        this.with = with as Binding
        this.dehydrated = closure.dehydrate()

    }

    DataflowChannel apply() {

        source.map {

            checkIsType(it, LinkedHashMap, method_name)
            def data = it as LinkedHashMap

            synchronized (this) {
                if (! keySet) {
                    keySet = data.keySet()
                }
            }

            mapChecks(data)

            def binding = new Binding(data)
            def rehydrated = dehydrated.rehydrate(with, binding, null)

            try {
                rehydrated.call()
            } catch(MissingPropertyException e) {
                tidyError("Unknown variable \"${e.getProperty()}\"\n" +
                    "data: ${data.toString()}, with: ${with.getVariables().toString()}", method_name)
            } catch (Exception e) {
                tidyError("${e.toString()}\n" +
                    "data: ${data.toString()}, with: ${with.getVariables().toString()}", method_name)
            }

            binding.getVariables() as LinkedHashMap
        }
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, method_name)
    }
}
