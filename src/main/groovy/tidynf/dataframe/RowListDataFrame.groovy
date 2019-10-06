package tidynf.dataframe


import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException
import tidynf.exception.TypeMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allKeySetsMatch
import static tidynf.helpers.Predicates.isListOfMap
import static tidynf.helpers.Predicates.isListOfMapOfSameType

class RowListDataFrame implements DataFrame {

    private ArrayList data
    private LinkedHashSet keySet

    RowListDataFrame(List data) {

        if (!isListOfMap(data))
            throw new IllegalTypeException(
                errMsg("${this.getClass().simpleName}", "Required List of Map\ngot: $data"))

        if (!allKeySetsMatch(data))
            throw new KeySetMismatchException(
                errMsg("${this.getClass().simpleName}", "Required matching keysets\nfirst keyset:${data[0].keySet()}"))

        if (!isListOfMapOfSameType(data))
            throw new TypeMismatchException(
                errMsg("${this.getClass().simpleName}", "Required matching data types for each variable"))

        this.data = data
        this.keySet = (data[0] as LinkedHashMap).keySet()
    }

    RowListDataFrame(LinkedHashMap data) {
        this.data = [data]
        this.keySet = data.keySet()
    }

    @Override
    String toString() {
        "[${this.getClass().simpleName} (${nrow()} x ${ncol()}):\n" + data.join('\n') + ']'
    }

    int nrow() {
        data.size()
    }

    int ncol() {
        keySet.size()
    }

    ArrayList as_list() {
        this.data
    }

    LinkedHashMap as_map() {
        transpose().as_map()
    }

    ColMapDataFrame transpose() {
        (keySet.collectEntries { k ->
            [(k): data.collect { it[k] }]
        } as LinkedHashMap) as ColMapDataFrame
    }

    RowListDataFrame mutate(Closure cl) {
        this.data.collect(cl) as RowListDataFrame
    }

    RowListDataFrame select(String... vars) {
        select(vars as Set)
    }

    RowListDataFrame select(Set vars) {
        data = data.collect { (it as LinkedHashMap).subMap(vars) }
        keySet = (data[0] as LinkedHashMap).keySet()
        this
    }

    ColMapDataFrame arrange(Map par = [:]) {
        transpose().arrange(par, keySet)
    }

    ColMapDataFrame arrange(Map par = [:], String... by) {
        transpose().arrange(par, by)
    }

    ColMapDataFrame arrange(Map par, Set by) {
        transpose().arrange(par, by)
    }


}
