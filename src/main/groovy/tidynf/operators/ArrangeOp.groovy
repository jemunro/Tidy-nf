
package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.TidyChecker.*


class ArrangeOp {

    private String method_name
    private DataflowChannel source
    private boolean reverse
    private List by
    private List at

    ArrangeOp(String method_name, Map params, DataflowChannel source, List by) {

        this.source = source
        this.method_name = method_name
        this.by = by

        def types = [at: List, at_: String, reverse: Boolean]
        def required = []
        checkRequiredParams(method_name, required, params)
        checkParamTypes(method_name, types, params)
        this.reverse = params?.reverse ?: false
        this.at = params?.at ?: []

    }

    DataflowChannel apply() {

        withKeys(source).map {

            runChecks(it)

            def data = it.data as LinkedHashMap

            def set = at ?
                (by + at).unique() :
                (data
                    .findAll { k, v -> data[k] instanceof List && ! by.contains(k) }
                    .findAll { it.value.size() == data[by[0]].size() }
                    .with { it.keySet() as List }
                    .with { by + it }
                )

            checkEqualSizes(set.collect { data[it] }, method_name)

            def sorted = set
                .collect { data[it] }
                .transpose()
                .collect { [it.take(by.size()), it.takeRight(it.size() - by.size())] }
                .sort { l1, l2 ->
                    [l1[0], l2[0]].transpose()
                        .find { e1, e2 -> e1 != e2 }
                        .with { it ? it[0] <=> it[1] : 0 } }
                .with { reverse ? it.reverse() : it }
                .collect { it[0] + it[1] }
                .transpose()
                .withIndex()
                .collectEntries { item, i -> [(set[i]) : item] }

            data.collectEntries { k, v -> [(k): sorted.containsKey(k) ? sorted[k] : it[k]] }
        }
    }

    void runChecks(LinkedHashMap map) {
        checkNonEmpty(by, method_name)
        checkIsType(map.keys, List, method_name)
        checkIsType(map.data, LinkedHashMap, method_name)
        checkHasKeys(map.data, by, method_name)
        checkKeysMatch(map.keys, map.data.keySet() as List, method_name)
        checkAllAreType(by.collect { map.data[it] }, List, method_name)
    }

}