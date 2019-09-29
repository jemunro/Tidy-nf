package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.areSameSet
import static tidynf.helpers.Predicates.isType

class RenameOp {
    private String methodName = 'rename'
    private DataflowChannel source
    private String newKey
    private String oldKey
    private LinkedHashSet keySet

    RenameOp(DataflowChannel source, String newKey, String oldKey) {

        this.source = source
        this.newKey = newKey
        this.oldKey = oldKey
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

                    if (!keySet.contains(oldKey))
                        throw new KeySetMismatchException(errMsg(methodName, "key not present in keySet\n" +
                                "key: $oldKey, keyset: $keySet"))

                    if (keySet.contains(newKey))
                        throw new KeySetMismatchException(errMsg(methodName, "key already present in keySet\n" +
                                "key: $newKey, keyset: $keySet"))
                }
            }

            if (! areSameSet(keySet, data.keySet()))
                throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                        "\nfirst keyset: $keySet\nmismatch keyset: ${data.keySet()}"))

            data.collectEntries { k, v -> [(oldKey == k ? newKey: k): v] }
        }
    }
}