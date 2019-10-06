package tidynf.dataframe


import tidynf.exception.CollectionSizeMismatchException
import tidynf.exception.IllegalTypeException
import tidynf.exception.TypeMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allAreListOfSameType
import static tidynf.helpers.Predicates.allAreSameSize
import static tidynf.helpers.Predicates.isMapOfList

class ColMapDataFrame implements DataFrame {

    private LinkedHashMap data
    private LinkedHashSet keySet

    ColMapDataFrame(LinkedHashMap data) {

        if (!isMapOfList(data))
            throw new IllegalTypeException(
                    errMsg("${this.getClass().simpleName}", "Required List of Map\ngot: $data"))

        if (!allAreSameSize(data.values()))
            throw new CollectionSizeMismatchException(
                    errMsg("${this.getClass().simpleName}", "Required all lists to be same size"))

        if (!allAreListOfSameType(data))
            throw new TypeMismatchException(
                    errMsg("${this.getClass().simpleName}", "Required matching data types for each variable"))

        this.data = data
        this.keySet = data.keySet()
    }

    @Override
    String toString() {
        "[${this.getClass().simpleName} (${nrow()} x ${ncol()}):\n" + as_list().join('\n') +']'
    }

    int nrow() {
        data[keySet[0]].size()
    }

    int ncol() {
        keySet.size()
    }

    ArrayList as_list() {
        transpose().as_list()
    }

    LinkedHashMap as_map() {
        this.data
    }

    RowListDataFrame transpose() {

        (0..<nrow())
            .collect { i ->
                data.keySet().collectEntries { k -> [(k): data[k][i]] }
            } as RowListDataFrame
    }

    ColMapDataFrame mutate(Closure cl) {
        transpose().mutate(cl).transpose()
    }

    ColMapDataFrame select(String... vars) {
        select(vars as Set)
    }

    ColMapDataFrame select(Set vars) {
        data = data.subMap(vars) as LinkedHashMap
        keySet = data.keySet()
        this
    }

    ColMapDataFrame arrange(Map par = [:]) {
        arrange(par, keySet)
    }

    ColMapDataFrame arrange(Map par = [:], String... by) {
        arrange(par, by as Set)
    }

    ColMapDataFrame arrange(Map par, Set by) {

        final LinkedHashSet byAt = by + keySet
        final boolean reverse = par?.desc ?: false

        data = byAt.collect { data[it] }.transpose()
            .collect { [it.take(by.size()), it.takeRight(it.size() - by.size())] }
            .sort { l1, l2 ->
                [l1[0], l2[0]].transpose()
                    .find { e1, e2 -> e1 != e2 }
                    .with { it ? it[0] <=> it[1] : 0 }
            }
            .with { reverse ? it.reverse() : it }
            .collect { it[0] + it[1] }
            .transpose()
            .withIndex()
            .collectEntries { item, i -> [(byAt[i]): item] }
            .subMap(keySet) as LinkedHashMap

        this
    }
}
