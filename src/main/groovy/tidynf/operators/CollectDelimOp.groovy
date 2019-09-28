package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch
import static tidynf.exception.TidyError.tidyError
import static tidynf.io.DelimHandler.writeDelim

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

        if (! validMethods.contains(methodName)) {
            tidyError("unknown collect_delim method: $methodName", "collect_delim")
        }
    }

    DataflowVariable apply() {

        source.with { sort ? it.toSortedList() : it.toList() }.map {

            def list = it as ArrayList
            runChecks(list)
            writeDelim(list, file, delim, colNames, false)
            file.toPath()
        }
    }

    void runChecks(List list) {

        list.collect { checkIsType(it, LinkedHashMap, methodName) }

        if (list.size() > 0) {
            def keySet = (list[0] as LinkedHashMap).keySet() as LinkedHashSet
            list.collect { checkKeysMatch(keySet, (it as LinkedHashMap).keySet() as LinkedHashSet, methodName) }
        }
    }
}

