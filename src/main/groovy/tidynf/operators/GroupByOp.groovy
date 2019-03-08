package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static tidynf.TidyChecker.checkEqualSizes
import static tidynf.TidyChecker.checkIsSubset
import static tidynf.TidyChecker.checkIsSubset
import static tidynf.TidyChecker.checkIsSubset
import static tidynf.TidyChecker.tidyError
import static tidynf.TidyChecker.tidyError
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

    static DataflowChannel preGroupBy(Map params, DataflowQueue source, List by, String method = 'prepareForJoin') {

        def group_size = params?.group_size ?: false
        def group_size_key = params?.group_size_key ?: 'size'
        if (group_size) {
            if (by.contains(group_size_key)) {
                tidyError(method, "by must no contain group size key")
            }

            def group_size_tuples = group_size.map_tidy(method) {
                def by_gsk = by + group_size_key
                checkIsSubset(method, by_gsk, it.keySet() as List)
                checkEqualSizes(by_gsk.collect { k -> it[k] }, method)
                def n = it[group_size_key].size()
                (0..<n).collectEntries { i ->
                    [ (by.collect { k -> it[k][i] }) : it[group_size_key][i] ]
                }
            }

            source
                .map_tidy(method) { checkIsSubset(method, by, it.keySet() as List) }
                .merge(group_size_tuples) { d, gs -> [data: d, group_size: gs ] }
                .map {
                def group_tuple = by.collect { k -> it.data[k] }
                if (! it.gs.containsKey(group_tuple)) {
                    tidyError(method, "tuple not found in group_size: $group_tuple")
                }
                [ groupKey(group_tuple, it.gs[group_tuple]), it.data]
            }

        } else {
            source
                .map_tidy(method) {
                checkIsSubset(method, by, it.keySet() as List)
                [ by.collect { k -> it.data[k] }, it.data ]
            }
        }
    }

}