package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.io.DelimHandler.writeDelim
import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.*
import static tidynf.helpers.DataHelpers.arrange


class CollectDelimOp {

    private String methodName
    private DataflowChannel source
    private boolean sort
    private String delim
    private File file
    private Boolean colNames
    private final static LinkedHashSet validMethods =  ["collect_delim", "collect_tsv", "collect_csv"]

    CollectDelimOp(DataflowChannel source, File file, String delim, Boolean colNames, Boolean sort, String methodName) {

        this.source = source
        this.sort = sort
        this.methodName = methodName
        this.delim = delim
        this.file = file
        this.colNames = colNames

        assert validMethods.contains(methodName)
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

            writeDelim(data, file, delim, colNames, false)
            file.toPath()
        }
    }
}

