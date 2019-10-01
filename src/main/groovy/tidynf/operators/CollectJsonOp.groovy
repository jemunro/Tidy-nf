package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.dataframe.DataFrameMethods.arrange
import static tidynf.helpers.Predicates.allKeySetsMatch
import static tidynf.helpers.Predicates.allKeySetsSameOrder
import static tidynf.helpers.Predicates.isListOfMap
import static tidynf.io.JsonHandler.writeJson

class CollectJsonOp {

    private DataflowChannel source
    private boolean sort
    private File file

    private static final String methodName = 'collect_json'

    CollectJsonOp(DataflowChannel source, File file, Boolean sort) {

        this.source = source
        this.sort = sort
        this.file = file

    }

    DataflowVariable apply() {

        source.with {
            it instanceof DataflowQueue ? it.toList() : it
        }.map {
            ArrayList data = it

            if(! isListOfMap(data))
                throw new IllegalTypeException(
                        errMsg(methodName, "Required List of Map\ngot: $data"))

            if(! allKeySetsMatch(data))
                throw new KeySetMismatchException(
                        errMsg(methodName,"Required matching keysets\nfirst keyset:${data[0].keySet()}"))

            if (sort) {
                data = arrange(data)
            }

            if (! allKeySetsSameOrder(data)) {
                LinkedHashSet keySet = (data[0] as LinkedHashMap).keySet()
                data = data.collect { (it as LinkedHashMap).subMap(keySet) }
            }

            writeJson(it, file)
            file.toPath()
        }
    }
}

