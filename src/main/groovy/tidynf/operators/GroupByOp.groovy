package tidynf.operators


import groovyx.gpars.dataflow.DataflowQueue

import static tidynf.TidyChecks.checkHasKey
import static tidynf.TidyChecks.checkHasKeys
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyChecks.checkParamTypes
import static tidynf.TidyChecks.checkRequiredParams
import static tidynf.TidyDataFlow.withKeys
import static nextflow.Nextflow.groupKey
import static tidynf.TidyHelpers.keySetList

class GroupByOp {

    private String method_name
    private DataflowQueue source
    private String group_size
    private List by

    GroupByOp(String method_name, Map params, DataflowQueue source, List by){

        this.method_name = method_name
        this.source = source
        this.by = by

        def required = []
        def types = [group_size: String]
        checkRequiredParams(method_name, required, params)
        checkParamTypes(method_name, types, params)
        this.group_size = params?.group_size as String

        if (this.group_size) {
            this.by = (this.by + [this.group_size]).unique()
        }

    }

    DataflowQueue apply() {

        prepareGroupBy()
            .groupTuple(by:0)
            .map {
                keySetList(it[1][0])
                    .collectEntries { k ->
                        [ (k) : (by.contains(k) ? it[1][0][k] : it[1].collect { m -> m[k] } )] }
            }
    }

    DataflowQueue prepareGroupBy() {
        /*
        / Return DataflowQueue of size 2 tuples - first entry is grouping tuple, second entry is linkedHashMap Data
        */

        withKeys(source).map {

            runChecks(it)

            if (this.group_size) {
                runGroupSizeChecks(this.group_size, it.data)
                [ groupKey(by.collect { k -> it.data[k] }, it.data[this.group_size] as Integer), it.data ]
            } else {
                [ by.collect { k -> it.data[k] }, it.data ]
            }
        }
    }


    void runChecks(LinkedHashMap map) {

        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, keySetList(map.data), method_name)
        checkHasKeys(map.data, by, method_name)
    }


    void runGroupSizeChecks(String key, LinkedHashMap data) {
        checkHasKey(data, key, method_name)
    }

}