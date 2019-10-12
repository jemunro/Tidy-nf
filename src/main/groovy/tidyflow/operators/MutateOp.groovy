package tidyflow.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.areSameSet
import static tidyflow.helpers.Predicates.isType

class MutateOp {

    private String methodName = 'mutate'
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

            if (! isType(it, Map))
                throw new IllegalTypeException(errMsg(methodName, "Required Map type\n" +
                        "got ${it.getClass().simpleName} with value $it"))

            LinkedHashMap data = it as LinkedHashMap

            synchronized (this) {
                if (! keySet) {
                    keySet = data.keySet()
                }
            }

            if (! areSameSet(keySet, data.keySet()))
                throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                        "\nfirst keyset: $keySet\nmismatch keyset: ${data.keySet()}"))

            Binding binding = new Binding(data)

            dehydrated.rehydrate(with, binding, null).call()

            binding.getVariables() as LinkedHashMap
        }
    }
}
