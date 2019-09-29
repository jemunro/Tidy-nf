package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Checks.checkKeysMatch
import static tidynf.io.DelimHandler.writeDelim
import static tidynf.helpers.Predicates.*

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

        assert validMethods.contains(methodName)
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
                    writeDelim([data], file, delim, colNames, false)

                } else {

                    if (! areSameSet(keySet, data.keySet()))
                        throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                                "\nfirst keyset: $keySet\nmismatch keyset: ${data.keySet()}"))

                    writeDelim([data.subMap(keySet)], file, delim, colNames, true)
                }
            }
            data
        }
    }
}

