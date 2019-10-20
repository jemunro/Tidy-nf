package tidyflow.dataframe

import tidyflow.exception.CollectionSizeMismatchException
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.TypeMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.allAreListOfSameType
import static tidyflow.helpers.Predicates.allAreSameSize
import static tidyflow.helpers.Predicates.isMapOfList

class TransposedDataFrame {

    private LinkedHashMap data
    private LinkedHashSet colNames

    TransposedDataFrame(LinkedHashMap data) {

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
        this.colNames = data.keySet()
    }

    static new_trans_df(LinkedHashMap data){
        new TransposedDataFrame(data)
    }

    TransposedDataFrame arrange(Map par = [:]) {
        arrange(par, colNames)
    }

    TransposedDataFrame arrange(Map par = [:], String... by) {
        arrange(par, by as Set)
    }

    TransposedDataFrame arrange(Map par, Set by) {

        final LinkedHashSet byAt = by + colNames
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
            .subMap(colNames) as LinkedHashMap

        data as TransposedDataFrame
    }

    int nrow() {
        data[colNames[0]].size()
    }

    int ncol() {
        colNames.size()
    }

    LinkedHashMap as_map() {
        data
    }

    DataFrame transpose() {

        (0..<nrow())
            .collect { i ->
                data.keySet().collectEntries { k -> [(k): data[k][i]] }
            } as DataFrame
    }
}
