package tidyflow.operators


import groovyx.gpars.dataflow.DataflowQueue
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.areSameSet
import static tidyflow.helpers.Predicates.isType

class GroupByOp {

    private String methodName = 'group_by'
    private DataflowQueue source
    private LinkedHashSet keySetBy
    private LinkedHashSet keySet

    GroupByOp(DataflowQueue source, List keySetBy){

        this.source = source
        this.keySetBy = keySetBy

        assert keySetBy.size() > 0
    }

    DataflowQueue apply() {

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
                }
            }

            if (! areSameSet(keySet, data.keySet()))
                throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                        "\nfirst keyset: $keySet\nmismatch keyset: ${data.keySet()}"))

            [data.subMap(keySetBy), data ]

        }.groupTuple(by:0)
            .map {
                keySet.collectEntries { k ->
                    [ (k) : (keySetBy.contains(k) ? it[0][k] : it[1].collect { m -> m[k] } )]
                }
            }
    }

}