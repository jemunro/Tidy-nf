package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.areSameSet
import static tidynf.helpers.Predicates.isType

class SelectOp {

    private String methodName = 'select'
    private DataflowChannel source
    private LinkedHashSet keySetSelect
    private LinkedHashSet keySet

    SelectOp(DataflowChannel source, List keys){

        this.source = source
        this.keySetSelect = keys
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

                    if (! keySet.containsAll(keySetSelect))
                        throw new KeySetMismatchException(errMsg(methodName, "select keySet not all present in keySet\n" +
                                "select: $keySetSelect, keyset: $keySet"))
                }
            }

            if (! areSameSet(keySet, data.keySet()))
                throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                        "\nfirst keyset: $keySet\nmismatch keyset: ${data.keySet()}"))

            data.subMap(keySetSelect)
        }
    }
}