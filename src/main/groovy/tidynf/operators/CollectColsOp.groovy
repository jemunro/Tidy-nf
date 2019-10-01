package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.dataframe.DataFrameMethods.arrange
import static tidynf.exception.Message.errMsg
import static tidynf.dataframe.DataFrameMethods.transpose
import static tidynf.helpers.Predicates.allKeySetsMatch
import static tidynf.helpers.Predicates.isListOfMap

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
                data = arrange(data)
            }

            transpose(data)
        }
    }
}