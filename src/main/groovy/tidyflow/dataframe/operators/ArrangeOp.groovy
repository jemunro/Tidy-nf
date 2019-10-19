
package tidyflow.dataframe.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.dataframe.DataFrame
import tidyflow.exception.CollectionSizeMismatchException
import tidyflow.exception.EmptySetException
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.allAreSameSize
import static tidyflow.helpers.Predicates.allAreType
import static tidyflow.helpers.Predicates.areSameSet
import static tidyflow.helpers.Predicates.isEmpty
import static tidyflow.helpers.Predicates.isType


class ArrangeOp {

    private String methodName = 'arrange'
    private DataflowChannel source
    private boolean reverse
    private LinkedHashSet keySetBy
    private LinkedHashSet keySetAt
    private LinkedHashSet keySet
    private LinkedHashSet keySetByAt

    ArrangeOp(Map params, DataflowChannel source, List keySetBy) {

        this.source = source
        this.keySetBy = keySetBy
        this.reverse = params?.reverse ?: false
        this.keySetAt = params?.at as List ?: []

        if (isEmpty(keySetBy))
            throw new EmptySetException(errMsg(methodName, "keyset by must not be empty"))
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

                    if (! keySet.containsAll(keySetBy))
                        throw new KeySetMismatchException(errMsg(methodName, "by keyset not all present in keyset\n" +
                                "by keyset: $keySetBy, keyset: $keySet"))

                    if (! keySetAt) {
                        keySetAt = data
                                .findAll { k, v -> v instanceof List && ! keySetBy.contains(k) }
                                .findAll { k, v -> (v as List).size() == data[keySetBy[0]].size() }
                                .keySet()
                    }

                    keySetByAt = keySetBy + keySetAt
                }
            }

            if (! areSameSet(keySet, data.keySet()))
                throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                        "\nfirst keyset: $keySet\nmismatch keyset: ${data.keySet()}"))

            if (! allAreType(keySetByAt.collect { k -> data[k] }, List))
                throw new IllegalTypeException(errMsg(methodName, "all selected variables must be lists\n" +
                        "${keySetByAt.collectEntries { k -> [(k) : data[k].getClass() ] } }"))

            if (! allAreSameSize(keySetByAt.collect { k -> data[k] } ))
                throw new CollectionSizeMismatchException(errMsg(methodName, "all selected variables in arrange must be the same size\n" +
                        "${keySetByAt.collectEntries { k -> [(k) : data[k]?.size() ] } }"))

            ([data] as DataFrame).arrange().as_list()[0]
        }
    }

}