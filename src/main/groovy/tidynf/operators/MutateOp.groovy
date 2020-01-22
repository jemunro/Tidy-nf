package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.areSameSet
import static tidynf.helpers.Predicates.isType

class MutateOp {

    private String methodName = 'mutate'
    private DataflowChannel source
    private Binding with
    private Closure dehydrated
    private LinkedHashSet keySet

    MutateOp(DataflowChannel source, Closure closure, Map with){

        this.source = source
        this.with = new Binding(with.clone() as Map)
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

            Binding binding = new Binding(data.clone() as LinkedHashMap)

            dehydrated.rehydrate(with, binding, null).call()

            binding.getVariables() as LinkedHashMap
        }
    }
}
