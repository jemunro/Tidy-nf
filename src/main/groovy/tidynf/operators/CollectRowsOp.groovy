package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import tidynf.dataframe.RowListDataFrame
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allKeySetsMatch
import static tidynf.helpers.Predicates.allKeySetsSameOrder
import static tidynf.helpers.Predicates.isListOfMap

class CollectRowsOp {

    private static final String methodName = 'collect_rows'
    private DataflowQueue source
    private boolean sort

    CollectRowsOp(DataflowQueue source, Boolean sort) {

        this.source = source
        this.sort = sort
    }

    DataflowVariable apply() {

        source.toList().map {
            ArrayList data = it

            if(! isListOfMap(data))
                throw new IllegalTypeException(
                        errMsg(methodName, "Required List of Map\ngot: $data"))

            if(! allKeySetsMatch(data))
                throw new KeySetMismatchException(
                        errMsg(methodName,"Required matching keysets\nfirst keyset:${data[0].keySet()}"))

            if (sort) {
                data = (data as RowListDataFrame).arrange().as_list()
            }

            if (! allKeySetsSameOrder(data)) {
                LinkedHashSet keySet = (data[0] as LinkedHashMap).keySet()
                data = (data as RowListDataFrame).select(keySet).as_list()
            }

            data
        }
    }
}