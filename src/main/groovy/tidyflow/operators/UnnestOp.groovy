package tidyflow.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.exception.CollectionSizeMismatchException
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.allAreSameSize
import static tidyflow.helpers.Predicates.allAreType
import static tidyflow.helpers.Predicates.areSameSet
import static tidyflow.helpers.Predicates.isType

class UnnestOp {

    private String methodName = 'unnest'
    private DataflowChannel source
    private LinkedHashSet keysAt
    private LinkedHashSet keySet


    UnnestOp(DataflowChannel source, List keysAt){

        this.source = source
        this.keysAt = keysAt
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

                    if (! keysAt) {
                        keysAt = data.findAll { k, v -> v instanceof List }.collect { it.key }
                    }

                    if (! keySet.containsAll(keysAt))
                        throw new KeySetMismatchException(errMsg(methodName, "at keys not all present in keySet\n" +
                                "at keys: $keysAt, keySet: $keySet"))
                }
            }

            if (! areSameSet(keySet, data.keySet()))
                throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                        "\nfirst keyset: $keySet\nmismatch keyset: ${data.keySet()}"))

            if (keysAt) {
                if (! allAreType( keysAt.collect { k -> data[k] }, List))
                    throw new IllegalTypeException(errMsg(methodName, "all selected variables must be lists\n" +
                            "${keysAt.collectEntries { k -> [(k) : data[k].getClass() ] } }"))

                if (! allAreSameSize( keysAt.collect { k -> data[k] } ))
                    throw new CollectionSizeMismatchException(errMsg(methodName, "all selected variables in unnest must be the same size\n" +
                            "${keysAt.collectEntries { k -> [(k) : data[k]?.size() ] } }"))

                int n = data[keysAt[0]].size()

                (0..<n).collect { i -> keySet.collectEntries { k -> [(k): keysAt.contains(k) ? data[k][i] : data[k]] } }

            } else {
                [ data ]
            }

        }.flatMap { it }
    }
}