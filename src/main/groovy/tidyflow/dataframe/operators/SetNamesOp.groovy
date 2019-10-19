package tidyflow.dataframe.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.exception.CollectionSizeMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.areSameSize
import static tidyflow.helpers.Predicates.isType

class SetNamesOp {

    private String methodName = 'set_names'
    private DataflowChannel source
    private LinkedHashSet keySet


    SetNamesOp(DataflowChannel source, List keySet){

        this.source = source
        this.keySet = keySet
    }

    DataflowChannel apply() {

        source.map {

            ArrayList list
            if (isType(it, List)) {
                list = it as ArrayList
            } else if(isType(it, LinkedHashMap)) {
                list = (it as LinkedHashMap).values()
            } else {
                list = [it]
            }

            if (! areSameSize(list, keySet))
                throw new CollectionSizeMismatchException(errMsg(methodName, "keySet and values are not same size\n" +
                        "keySet: $keySet, values: $list"))

            [ keySet as ArrayList, list ]
                .transpose()
                .collectEntries { k, v -> [(k): v] }
        }
    }
}