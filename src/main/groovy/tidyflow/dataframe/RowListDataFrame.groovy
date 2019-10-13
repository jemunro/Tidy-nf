package tidyflow.dataframe

import sun.awt.image.ImageWatched
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException
import tidyflow.exception.TypeMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.allAreType
import static tidyflow.helpers.Predicates.allKeySetsMatch
import static tidyflow.helpers.Predicates.isListOfMap
import static tidyflow.helpers.Predicates.isListOfMapOfSameType

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

    ColMapDataFrame arrange(Map par = [:]) {
        transpose().arrange(par, keySet)
    }

    ColMapDataFrame arrange(Map par = [:], String... by) {
        transpose().arrange(par, by)
    }

    ColMapDataFrame arrange(Map par, Set by) {
        transpose().arrange(par, by)
    }

    ArrayList as_list() {
        data
    }

    LinkedHashMap as_map() {
        transpose().as_map()
    }

    RowListDataFrame full_join(DataFrame right, String... by) {
        full_join(right, by as Set)
    }


    RowListDataFrame full_join(DataFrame right, Set by) {

        if (right instanceof ColMapDataFrame) {
            right = right.transpose()
        }

        ArrayList right_sub = right.select(by).as_list()

        ArrayList matches = select(by)
            .as_list()
            .withIndex()
            .collect { l, i -> right_sub.withIndex().findAll { r, j -> l == r }.collect { [i, it[1]] } }
            .collectMany { it }
    }

    AbstractDataFrame full_join(AbstractDataFrame right, String... by) {
        full_join(right, by as Set)
    }


    AbstractDataFrame full_join(AbstractDataFrame right, Set by) {
        if (right instanceof ColMapDataFrame) {
            null
        }

    }

    Set names() {
        keySet
    }

    int nrow() {
        data.size()
    }

    int ncol() {
        keySet.size()
    }

    RowListDataFrame mutate(Closure closure) {
        data.collect {
            Binding binding = new Binding(it as LinkedHashMap)
            closure.rehydrate(closure.delegate, binding, closure.thisObject).call()
            binding.getVariables() as LinkedHashMap
        } as RowListDataFrame
    }

    RowListDataFrame mutate_with(Map with = [:], Closure closure) {
        data.collect {
            Binding binding = new Binding(it as LinkedHashMap)
            Binding withBinding = new Binding(with)
            closure.rehydrate(withBinding, binding, closure.thisObject).call()
            binding.getVariables() as LinkedHashMap
        } as RowListDataFrame
    }

    @Override
    String toString() {

        if (data.size() < 6)
            "[${this.getClass().simpleName} (${nrow()} x ${ncol()}):\n" + data.join('\n') + ']'
        else
            "[${this.getClass().simpleName} (${nrow()} x ${ncol()}):\n" + data.subList(0, 5).join('\n') + '\n[... ]]'
    }


    ColMapDataFrame transpose() {
        (keySet.collectEntries { k ->
            [(k): data.collect { it[k] }]
        } as LinkedHashMap) as ColMapDataFrame
    }


    RowListDataFrame rename(Map nameMap) {
        if (!allAreType(nameMap.values(), String)) {
            throw new IllegalTypeException(
                errMsg("rename", "all from values must be strings.\n" +
                    "from: ${nameMap.values()}"))
        }

        if (!keySet.containsAll(nameMap.values())) {
            throw new KeySetMismatchException(
                errMsg("rename", "names from not all present in keyset.\n" +
                    "from: ${nameMap.values()}, keyset: ${keySet}"))
        }

        if (keySet.any { nameMap.keySet().contains(it) }) {
            throw new KeySetMismatchException(
                errMsg("rename", "some of names to not present in keyset.\n" +
                    "to: ${nameMap.keySet()}, keyset: ${keySet}"))
        }
        Map invNameMap = nameMap.collectEntries { k, v -> [(v): k] }

        data.collect {
            keySet.collectEntries { k ->
                [(invNameMap.containsKey(k) ? invNameMap[k] : k): it[k]]
            } as LinkedHashMap
        } as RowListDataFrame
    }

    RowListDataFrame select(String... vars) {
        select(vars as Set)
    }

    RowListDataFrame select(Set vars) {

        if (!keySet.containsAll(vars)) {
            throw new KeySetMismatchException(
                errMsg("select", "names not all present in keyset.\n" +
                    "names: ${vars}, keyset: ${keySet}"))
        }

        data.collect { (it as LinkedHashMap).subMap(vars) } as RowListDataFrame
    }


}
