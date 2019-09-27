package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecks.checkEqualSizes
import static tidynf.TidyChecks.checkUnique
import static tidynf.TidyHelpers.coerceToList

class SetNamesOp {

    private String method_name = 'set_names'
    private DataflowChannel source
    private List keys


    SetNamesOp(DataflowChannel source, List keys){

        this.source = source
        this.keys = keys

    }

    DataflowChannel apply(){

        checkUnique(keys, method_name)

        source.map {

            def list = coerceToList(it, method_name)

            mapChecks(list)

            [keys, list]
                .transpose()
                .collectEntries { k, v -> [(k): v] }
        }
    }

    void mapChecks(List list) {
        checkEqualSizes(list, keys, method_name)
    }
}