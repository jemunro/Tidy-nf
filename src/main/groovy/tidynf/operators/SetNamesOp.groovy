package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkUnique
import static tidynf.TidyChecks.requireAsList

class SetNamesOp {

    private String method_name
    private DataflowChannel source
    private List keys


    SetNamesOp(String method_name, DataflowChannel source, List keys){

        this.method_name = method_name
        this.source = source
        this.keys = keys

    }

    DataflowChannel apply(){

        checkUnique(keys, method_name)

        source.map {

            def list = requireAsList(it, method_name)

            [keys, list]
                .transpose()
                .collectEntries { k, v -> [(k): v] }
        }
    }
}