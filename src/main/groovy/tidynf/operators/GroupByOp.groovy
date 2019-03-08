package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.exception.TidyError.error

class GroupByOp {

    private String method_name
    private DataflowQueue source
    private DataflowVariable group_size
    private List by

    GroupByOp(String method_name, Map params, DataflowQueue queue, List by){

        this.method_name = method_name
        this.source = source
        this.by = by

        def required = []
        def types = [group_size: DataflowVariable, group_size_key: String]
        checkRequiredParams(method_name, required, params)
        checkParamTypes(method_name, types, params)
        this.group_size = params.group_size

    }

    DataflowQueue apply() {

        def required = []
        def types = [group_size: DataflowVariable, group_size_key: String]
        checkRequiredParams(method_name, required, params)
        checkParamTypes(method_name, types, params)


        withKeys(source).map {
            it.data = requireAsLinkedHashMap(method_name, it.data)
            checkKeysMatch(method_name, it.data.keySet() as List, it.keys as List)
            checkKeysAreSubset(method_name, by, it.keys)
            [ ( by.size() > 1 ? by.collect { k -> it.data[k] } : it.data[by[0]] ), it.data ]
        }.groupTuple().map {
            it[1][0].keySet().collectEntries { k ->
                [(k): (by.contains(k) ?
                    (group_key ? groupKey(it[1][0][k], it[1].size()) : it[1][0][k] ) :
                    ( it[1].collect { it[k] } )
                )]
            }
        }
    }

}