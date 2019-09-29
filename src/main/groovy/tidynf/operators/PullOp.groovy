package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.areSameSet
import static tidynf.helpers.Predicates.isType

class PullOp {

    private String methodName = 'pull'
    private DataflowChannel source
    private String key
    private LinkedHashSet keySet

    PullOp(DataflowChannel source, String key) {

        this.source = source
        this.key = key
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

                    if (!keySet.contains(key))
                        throw new KeySetMismatchException(errMsg(methodName, "key not present in keySet\n" +
                                "key: $key, keyset: $keySet"))
                }
            }

            if (! areSameSet(keySet, data.keySet()))
                throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                        "\nfirst keyset: $keySet\nmismatch keyset: ${data.keySet()}"))

            data[key]
        }
    }
}