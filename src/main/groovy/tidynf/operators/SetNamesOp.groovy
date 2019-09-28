package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.helpers.TidyChecks.checkEqualSizes
import static tidynf.helpers.TidyChecks.checkUnique
import static tidynf.helpers.TidyHelpers.coerceToList

class SetNamesOp {

    private String methodName = 'set_names'
    private DataflowChannel source
    private List keys


    SetNamesOp(DataflowChannel source, List keys){

        this.source = source
        this.keys = keys

    }

    DataflowChannel apply(){

        checkUnique(keys, methodName)

        source.map {

            def list = coerceToList(it, methodName)

            mapChecks(list)

            [keys, list]
                .transpose()
                .collectEntries { k, v -> [(k): v] }
        }
    }

    void mapChecks(List list) {
        checkEqualSizes(list, keys, methodName)
    }
}