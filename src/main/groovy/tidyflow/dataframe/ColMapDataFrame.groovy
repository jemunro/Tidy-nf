package tidyflow.dataframe


import tidyflow.exception.CollectionSizeMismatchException
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException
import tidyflow.exception.TypeMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.allAreListOfSameType
import static tidyflow.helpers.Predicates.allAreSameSize
import static tidyflow.helpers.Predicates.allAreType
import static tidyflow.helpers.Predicates.isMapOfList

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
        "[${this.getClass().simpleName} (${nrow()} x ${ncol()}):\n" + as_list().join('\n') + ']'

        if (data[keySet[0]].size() < 6)
            "[${this.getClass().simpleName} (${nrow()} x ${ncol()}):\n" + as_list().join('\n') + ']'
        else
            "[${this.getClass().simpleName} (${nrow()} x ${ncol()}):\n" + as_list().subList(0, 5).join('\n') + '\n[... ]]'
    }

    int nrow() {
        data[keySet[0]].size()
    }

    int ncol() {
        keySet.size()
    }

    Set names() {
        this.keySet
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

    RowListDataFrame mutate( Closure closure) {
        transpose().mutate(closure)
    }

    RowListDataFrame mutate_with(Map with = [:], Closure closure) {
        transpose().mutate_with(with, closure)
    }

    RowListDataFrame rename(Map nameMap) {
        transpose().rename(nameMap)
    }

    ColMapDataFrame select(String... vars) {
        select(vars as Set)
    }

    ColMapDataFrame select(Set vars) {

        if (!keySet.containsAll(vars)) {
            throw new KeySetMismatchException(
                errMsg("select", "names not all present in keyset.\n" +
                    "names: ${vars}, keyset: ${keySet}"))
        }

        (data.subMap(vars) as LinkedHashMap) as ColMapDataFrame
    }

    RowListDataFrame slice(int... rows){
        slice(rows as ArrayList)
    }

    RowListDataFrame slice(IntRange rows){
        slice(rows as ArrayList)
    }

    RowListDataFrame slice(ArrayList rows){
        transpose().slice(rows)
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

        LinkedHashMap data = byAt.collect { data[it] }.transpose()
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

        data as ColMapDataFrame
    }

    ColMapDataFrame full_join(DataFrame right, String... by) {
        transpose().full_join(right, by as Set).transpose()
    }


    ColMapDataFrame full_join(DataFrame right, Set by) {
        transpose().full_join(right, by).transpose()
    }

    AbstractDataFrame full_join(AbstractDataFrame right, String... by) {
        transpose().full_join(right, by as Set).transpose()
    }


    AbstractDataFrame full_join(AbstractDataFrame right, Set by) {
        transpose().full_join(right, by).transpose()
    }
}
