
package tidynf.operators

import groovyx.gpars.dataflow.DataflowWriteChannel
import java.nio.file.Path

import static tidynf.TidyChecks.checkAllAreType
import static tidynf.TidyChecks.checkHasKeys
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyChecks.checkNonEmpty
import static tidynf.TidyChecks.checkParamTypes
import static tidynf.TidyChecks.checkRequiredParams
import static tidynf.TidyDataFlow.withKeys
import static tidynf.TidyHelpers.keySetList


class ArrangeOp {

    private String method_name
    private DataflowWriteChannel source
    private boolean reverse
    private List by

    ArrangeOp(String method_name, Map params, DataflowWriteChannel source, List by) {

        this.source = source
        this.method_name = method_name
        this.by = by

        def types = [reverse: Boolean]
        def required = []
        checkRequiredParams(method_name, required, params)
        checkParamTypes(method_name, types, params)
        this.reverse = params?.reverse ?: false

    }

    DataflowWriteChannel apply() {

        withKeys(source).map {

            runChecks(it)

            def data = it.data as LinkedHashMap
            def n = (data[by[0]] as List).size()

            List order = (by.collect { data [it] } + [0..(n-1) as List ])
                .collectNested { it instanceof Path ? (it as Path).fileName.toString() : it }
                .transpose()
                .toSorted { l1, l2 ->
                    [l1, l2].transpose()
                        .find { e1, e2 -> e1 != e2 }
                        .with { e1, e2 -> e1 <=> e2 } }
                .transpose()
                .takeRight(1)[0]

            if (reverse) {
                order = order.reverse()
            }

            data.collectEntries { k, v -> [(k): by.contains(k) ? v[order] : v ] }
        }
    }

    void runChecks(LinkedHashMap map) {
        checkNonEmpty(by, method_name)
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkHasKeys(map.data, by, method_name)
        checkKeysMatch(map.keys, keySetList(map.data), method_name)
        checkAllAreType(by.collect { map.data[it] }, List, method_name)
    }

}