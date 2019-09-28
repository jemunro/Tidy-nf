package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue

import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch
import static tidynf.exception.TidyError.tidyError
import static tidynf.io.DelimHandler.writeDelim

class SubscribeDelimOp {

    private String methodName
    private DataflowQueue source
    private String delim
    private File file
    private Boolean colNames
    private LinkedHashSet keySet
    private final static LinkedHashSet validMethods =  ["subscribe_delim", "subscribe_tsv", "subscribe_csv"]

    SubscribeDelimOp(DataflowQueue source, File file, String delim, Boolean colNames, String methodName) {

        this.source = source
        this.methodName = methodName
        this.delim = delim
        this.file = file
        this.colNames = colNames

        if (! validMethods.contains(methodName)) {
            tidyError("unknown subscribe_delim method: $methodName", "subscribe_delim")
        }
    }

    DataflowQueue apply() {

        source.map {

            checkIsType(it, LinkedHashMap, methodName)
            def data = it as LinkedHashMap

            synchronized (this) {
                if (! keySet) {
                    keySet = data.keySet()
                    writeDelim([data], file, delim, colNames, false)
                } else {
                    mapChecks(data)
                    writeDelim([data], file, delim, colNames, true)
                }
            }
            data
        }
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
    }
}

