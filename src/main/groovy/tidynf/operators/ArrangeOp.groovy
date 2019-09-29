
package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.helpers.Checks.checkAllAreType
import static tidynf.helpers.Checks.checkContainsAll
import static tidynf.helpers.Checks.checkEqualSizes
import static tidynf.helpers.Checks.checkIsType
import static tidynf.helpers.Checks.checkKeysMatch
import static tidynf.helpers.Checks.checkNonEmpty
import static tidynf.helpers.Checks.checkParamTypes
import static tidynf.helpers.Checks.checkRequiredParams


class ArrangeOp {

    private String methodName = 'arrange'
    private DataflowChannel source
    private boolean reverse
    private List by
    private List at
    private LinkedHashSet keySet

    ArrangeOp(Map params, DataflowChannel source, List by) {

        this.source = source
        this.by = by

        def types = [at: List, at_: String, reverse: Boolean]
        def required = []
        checkRequiredParams(methodName, required, params)
        checkParamTypes(methodName, types, params)
        this.reverse = params?.reverse ?: false
        this.at = params?.at as List ?: []

    }

    DataflowChannel apply() {

        source.map {

            checkIsType(it, LinkedHashMap, methodName)
            def data = it as LinkedHashMap

            synchronized (this) {
                if (! keySet) {
                    keySet = data.keySet()
                    firstChecks()
                }
            }

            mapChecks(data)

            def set = at ?
                (by + at).unique() :
                (data
                    .findAll { k, v -> data[k] instanceof List && ! by.contains(k) }
                    .findAll { it.value.size() == data[by[0]].size() }
                    .with { it.keySet() as ArrayList }
                    .with { by + it }
                )

            checkEqualSizes(set.collect { data[it] }, methodName)

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

            data.collectEntries { k, v -> [(k): sorted.containsKey(k) ? sorted[k] : v ] }
        }
    }

    void firstChecks() {
        checkNonEmpty(by, methodName)
        checkContainsAll(keySet, by, methodName)
    }

    void mapChecks(LinkedHashMap data) {
        checkKeysMatch(keySet, data.keySet() as LinkedHashSet, methodName)
        checkAllAreType(by.collect { data[it] }, List, methodName)
    }

}