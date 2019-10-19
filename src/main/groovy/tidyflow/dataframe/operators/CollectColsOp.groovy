package tidyflow.dataframe.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import tidyflow.dataframe.DataFrame
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.allKeySetsMatch
import static tidyflow.helpers.Predicates.isListOfMap

class CollectColsOp {

    private static final String methodName = 'collect_cols'
    private DataflowQueue source
    private boolean sort

    CollectColsOp(DataflowQueue source, boolean sort) {

        this.source = source
        this.sort = sort
    }

    DataflowVariable apply() {

        source.toList().map {

            ArrayList data = it

            if (!isListOfMap(data))
                throw new IllegalTypeException(
                        errMsg(methodName, "Required List of Map\ngot: $data"))

            if (!allKeySetsMatch(data))
                throw new KeySetMismatchException(
                        errMsg(methodName, "Required matching keysets\nfirst keyset:${data[0].keySet()}"))

            if (sort) {
                data = (data as DataFrame).arrange().as_list()
            }

            (data as DataFrame).as_map()
        }
    }
}