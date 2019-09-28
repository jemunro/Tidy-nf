package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.io.JsonHandler.writeJson

class CollectJsonOp {

    private DataflowQueue source
    private boolean sort
    private File file

    private static final String methodName = 'collect_json'

    CollectJsonOp(DataflowQueue source, File file, Boolean sort) {

        this.source = source
        this.sort = sort
        this.file = file

    }

    DataflowVariable apply() {

        source.with {
            it instanceof DataflowQueue ?
                ( sort ? it.toSortedList() : it.toList() ) : it
        }.map {
            writeJson(it, file)
            file.toPath()
        }
    }
}

