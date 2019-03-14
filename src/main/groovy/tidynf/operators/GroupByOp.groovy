package tidynf.operators


import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.TidyChecks.checkEqualSizes
import static tidynf.TidyChecks.checkHasKey
import static tidynf.TidyChecks.checkHasKeys
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyChecks.checkParamTypes
import static tidynf.TidyChecks.checkRequiredParams
import static tidynf.exception.TidyError.error
import static tidynf.TidyDataFlow.withKeys
import static nextflow.Nextflow.groupKey
import static tidynf.TidyHelpers.keySetList

class GroupByOp {

    private String method_name
    private DataflowQueue source
    private DataflowVariable group_size
    private String group_size_key
    private List by

    GroupByOp(String method_name, Map params, DataflowQueue source, List by){

        this.method_name = method_name
        this.source = source
        this.by = by

        def required = []
        def types = [group_size: DataflowVariable, group_size_key: String]
        checkRequiredParams(method_name, required, params)
        checkParamTypes(method_name, types, params)
        this.group_size = params?.group_size
        this.group_size_key = params?.group_size_key?: 'size'

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

            [ by.collect { k -> it.data[k] }, it.data ]

        }.with {
            if (group_size) {

                runGroupSizeChecks(group_size)

                def group_size_tuples = group_size.map {

                    def n = it[group_size_key].size()

                    (0..<n).collectEntries { i ->
                        [ (by.collect { k -> it[k][i] }) : it[group_size_key][i] ]
                    }
                }

                it.merge(group_size_tuples) { d, m  ->

                    if (! m?.containsKey(d[0])) {
                        error("Grouping tuple: ${d[0]} not found in group_size_tuples ${m}.", method_name)
                    }

                    [ groupKey(d[0], m[d[0]]), d[1] ]
                }
            } else {
                it
            }
        }

    }


    void runChecks(LinkedHashMap map) {

        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkKeysMatch(map.keys, keySetList(map.data), method_name)
        checkHasKeys(map.data, by, method_name)
    }


    void runGroupSizeChecks(DataflowVariable var){

        if (by.contains(group_size_key)) {
            error("group_size_key ($group_size_key) must not be in by ($by)", method_name)
        }

        var.map {
            checkIsType(it, LinkedHashMap, method_name)
            checkHasKeys(it as LinkedHashMap, by, method_name)
            checkHasKey(it as LinkedHashMap, group_size_key, method_name)
            checkEqualSizes((by + group_size_key).collect { k -> it[k] }, method_name)
        }
    }

}