package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.io.DelimHandler.writeDelim
import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.*

class CollectDelimOp {

    private String methodName
    private DataflowQueue source
    private boolean sort
    private String delim
    private File file
    private Boolean colNames
    private final static LinkedHashSet validMethods =  ["collect_delim", "collect_tsv", "collect_csv"]

    CollectDelimOp(DataflowQueue source, File file, String delim, Boolean colNames, Boolean sort, String methodName) {

        this.source = source
        this.sort = sort
        this.methodName = methodName
        this.delim = delim
        this.file = file
        this.colNames = colNames

        assert validMethods.contains(methodName)
    }

    DataflowVariable apply() {

        source.with { sort ? it.toSortedList() : it.toList() }.map {

            ArrayList data = it

            if(! isListOfMap(data))
                throw new IllegalTypeException(
                        errMsg(methodName, "Required List of Map\ngot: $data"))

            if(! allKeySetsMatch(data))
                throw new KeySetMismatchException(
                        errMsg(methodName,"Required matching keysets\nfirst keyset:${data[0].keySet()}"))

            if (! allKeySetsSameOrder(data)) {
                LinkedHashSet keySet = (data[0] as LinkedHashMap).keySet()
                data = data.collect { (it as LinkedHashMap).subMap(keySet) }
            }

            writeDelim(data, file, delim, colNames, false)
            file.toPath()
        }
    }
}

