package tidynf.operators

import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.DataflowWriteChannel

import static tidynf.TidyChecks.checkUnique
import static tidynf.TidyChecks.requireAsList

class SetNamesOp {

    private String method_name
    private DataflowWriteChannel source
    private List keys


    SetNamesOp(String method_name, DataflowWriteChannel source, List keys){

        this.method_name = method_name
        this.source = source
        this.keys = keys

    }

    DataflowWriteChannel apply(){

        checkUnique(keys, method_name)

        source.map {

            def list = requireAsList(it, method_name)

            [keys, list]
                .transpose()
                .collectEntries { k, v -> [(k): v] }
        }
    }
}