package tidyflow.dataframe

import tidyflow.exception.CollectionSizeMismatchException
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException
import tidyflow.exception.TypeMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.allAllAreSameSize
import static tidyflow.helpers.Predicates.allAreType
import static tidyflow.helpers.Predicates.allKeySetsMatch
import static tidyflow.helpers.Predicates.isListOfMap
import static tidyflow.helpers.Predicates.isListOfMapOfSameType

class DataFrame implements AbstractDataFrame {

    private ArrayList data
    private LinkedHashSet colNames
    private boolean isPartial = false

    DataFrame(List data) {

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
        this.colNames = (data[0] as LinkedHashMap).keySet()
    }

    static DataFrame new_df(ArrayList data) {
        data as DataFrame
    }

    static DataFrame new_df(LinkedHashMap data) {
        [data] as DataFrame
    }


    @Override
    DataFrame nest_by(Map par = [:], String... by) {
        nest_by(par, by as Set)
    }

    @Override
    DataFrame nest_by(Map par = [:], Set by) {

        if (!colNames.containsAll(by)) {
            throw new KeySetMismatchException(
                    errMsg("nest_by", "by not all present in colnames.\n" +
                            "by: $by, colnames: ${colNames}"))
        }

        Set nest_cols = colnames() - by
        LinkedHashMap groups = [:]
        select(by).as_list().withIndex().forEach { row, idx ->
            groups[row] = (groups[row] ?: []) + idx
        }
        ArrayList result = groups.collect { k, v ->
            (k as LinkedHashMap) + nest_cols.collectEntries { name ->
                [(name): v.collect { i -> data[i][name] }]
            }
        }
        result as DataFrame
    }

    @Override
    DataFrame arrange(Map par = [:], String... by) {
        arrange(par, by as Set)
    }

    @Override
    AbstractDataFrame arrange_all(Map par = [:]) {
        arrange(par, colNames)
    }

    @Override
    DataFrame arrange(Map par = [:], Set by) {

        if (!colNames.containsAll(by)) {
            throw new KeySetMismatchException(
                    errMsg("arrange", "by not all present in colnames.\n" +
                            "by: $by, colnames: ${colNames}"))
        }

        transpose().arrange(par, by).transpose()
    }



    ArrayList split_rows() {
        data.collect { [it] as DataFrame }
    }

    ArrayList as_list() {
        data
    }

    LinkedHashMap as_map() {
        transpose().as_map()
    }

    LinkedHashMap getAt(IntRange i){
        data[i]
    }

    LinkedHashMap getAt(List i){
        data[i]
    }

    LinkedHashMap getAt(Integer i){
        data[i]
    }

    ArrayList getAt(String var){
        data.collect { it[var] }
    }

    DataFrame inner_join(DataFrame right, String... by) {
        inner_join(right, by as Set)
    }

    DataFrame inner_join(DataFrame right, Set by) {
        join(right, by, Join.INNER)
    }

    DataFrame left_join(DataFrame right, String... by) {
        left_join(right, by as Set)
    }

    DataFrame left_join(DataFrame right, Set by) {
        join(right, by, Join.LEFT)
    }

    DataFrame right_join(DataFrame right, String... by) {
        right_join(right, by as Set)
    }

    DataFrame right_join(DataFrame right, Set by) {
        join(right, by, Join.RIGHT)
    }

    DataFrame full_join(DataFrame right, String... by) {
        full_join(right, by as Set)
    }

    DataFrame full_join(DataFrame right, Set by) {
        join(right, by, Join.FULL)
    }

    DataFrame semi_join(DataFrame right, String... by) {
        semi_join(right, by as Set)
    }

    DataFrame semi_join(DataFrame right, Set by) {
        join(right, by, Join.SEMI)
    }

    DataFrame anti_join(DataFrame right, String... by) {
        anti_join(right, by as Set)
    }

    DataFrame anti_join(DataFrame right, Set by) {
        join(right, by, Join.ANTI)
    }

    private DataFrame join(DataFrame right, Set by, Join join) {
        // TODO - support for zero length DataFrames as join output ?

        if (!colNames.containsAll(by)) {
            throw new KeySetMismatchException(
                errMsg("join", "by  not all present in keyset left.\n" +
                    "by: $by, keyset left: ${colNames}"))
        }

        if (!right.names().containsAll(by)) {
            throw new KeySetMismatchException(
                errMsg("join", "by  not all present in keyset right.\n" +
                    "by: $by, keyset right: ${right.names()}"))
        }

        ArrayList left_only_data = []
        ArrayList right_only_data = []
        ArrayList inner_data = []

        ArrayList right_sub = right.select(by).as_list()

        ArrayList intersect_lr = select(by)
            .as_list()
            .withIndex()
            .collect { l, i -> right_sub.withIndex().findAll { r, j -> l == r }.collect { [i, it[1]] } }
            .collectMany { it }

        ArrayList left_only_i = intersect_lr
            .collect { it [0] }
            .with { (0..<data.size()) - it }

        ArrayList right_only_i = intersect_lr
                .collect { it [1] }
                .with { (0..<right.nrow()) - it }


        if (join != Join.SEMI && join != Join.ANTI) {
            inner_data = intersect_lr.collect { i, j -> (data[i] + right[j]) as LinkedHashMap }
        } else {
            if (join == Join.SEMI) {
                inner_data = intersect_lr.collect { (data[it[0] as int]) as LinkedHashMap }
            }
            else if (join == Join.ANTI) {
                left_only_data = left_only_i.collect { (data[it as int]) as LinkedHashMap }
            }
        }

        if (join == Join.LEFT || join == Join.FULL) {
            LinkedHashMap right_null = (right.names() - by).collectEntries { k -> [(k): null]} as LinkedHashMap
            left_only_data = left_only_i.collect { (data[it as int] + right_null) as LinkedHashMap }
        }

        if (join == Join.RIGHT || join == Join.FULL) {
            LinkedHashMap left_null = (names() - by).collectEntries { k -> [(k): null]} as LinkedHashMap
            right_only_data = right_only_i.collect { (left_null + right[it as int]) as LinkedHashMap }
        }

        switch (join){
            case Join.ANTI:
                left_only_data as DataFrame
                break
            case Join.SEMI:
                inner_data as DataFrame
                break
            case Join.INNER:
                inner_data as DataFrame
                break
            case Join.LEFT:
                inner_data + left_only_data as DataFrame
                break
            case Join.RIGHT:
                inner_data + right_only_data as DataFrame
                break
            case Join.FULL:
                inner_data + left_only_data + right_only_data as DataFrame
                break
            default:
                null
        }
    }

    Set names() {
        colNames
    }

    Set colnames() {
        colNames
    }

    int nrow() {
        data.size()
    }

    int ncol() {
        colNames.size()
    }

    DataFrame mutate(Closure closure) {

        data.collect {
            Binding binding = new Binding(it.clone() as LinkedHashMap)
            closure.rehydrate(closure.delegate, binding, closure.thisObject).call()
            binding.getVariables() as LinkedHashMap
        } as DataFrame
    }

    DataFrame mutate_with(Map with = [:], Closure closure) {

        Binding withBinding = new Binding(with.clone() as LinkedHashMap)
        data.collect {
            Binding binding = new Binding(it.clone() as LinkedHashMap)
            closure.rehydrate(withBinding, binding, closure.thisObject).call()
            binding.getVariables() as LinkedHashMap
        } as DataFrame
    }

    ArrayList pull(String var){
        if (!colNames.contains(var)) {
            throw new KeySetMismatchException(
                errMsg("pull", "var not all present in keyset.\n" +
                    "var: $var, keyset: $colNames"))
        }
        data.collect { (it as LinkedHashMap)[var] }
    }

    DataFrame rename(Map nameMap) {
        if (!allAreType(nameMap.values(), String)) {
            throw new IllegalTypeException(
                errMsg("rename", "all from values must be strings.\n" +
                    "from: ${nameMap.values()}"))
        }

        if (!colNames.containsAll(nameMap.values())) {
            throw new KeySetMismatchException(
                errMsg("rename", "names from not all present in keyset.\n" +
                    "from: ${nameMap.values()}, keyset: ${colNames}"))
        }

        if (colNames.any { nameMap.keySet().contains(it) }) {
            throw new KeySetMismatchException(
                errMsg("rename", "some of names to not present in keyset.\n" +
                    "to: ${nameMap.keySet()}, keyset: ${colNames}"))
        }
        Map invNameMap = nameMap.collectEntries { k, v -> [(v): k] }

        data.collect {
            colNames.collectEntries { k ->
                [(invNameMap.containsKey(k) ? invNameMap[k] : k): it[k]]
            } as LinkedHashMap
        } as DataFrame
    }

    DataFrame select(String... vars) {
        select(vars as Set)
    }

    DataFrame select(Set vars) {

        if (!colNames.containsAll(vars)) {
            throw new KeySetMismatchException(
                errMsg("select", "names not all present in keyset.\n" +
                    "names: ${vars}, keyset: ${colNames}"))
        }

        data.collect { (it as LinkedHashMap).subMap(vars) } as DataFrame
    }

    @Override
    DataFrame unnest(String... at) {
        unnest(at as Set)
    }

    @Override
    DataFrame unnest(Set at) {

        if (!colNames.containsAll(at)) {
            throw new KeySetMismatchException(
                    errMsg("unnest", "at colnames not all present in DataFrame.\n" +
                            "names: ${at}, colnames: ${colNames}"))
        }

        if (! allAreType( at.collect { k -> data[0][k] }, List))
            throw new IllegalTypeException(errMsg("unnest", "all selected columns must be List\n" +
                    "${at.collectEntries { k -> [(k) : data[0][k].getClass() ] }}"))

        if (! allAllAreSameSize( data.collect { at.collect { k -> it[k] } } ))
            throw new CollectionSizeMismatchException(errMsg("unnest", "all selected columns must be the " +
                    "same size\n"))

        Set not_at = colnames() - at
        data.collectMany {row ->
            int size = row[at[0]].size()
            (0..<size).collect { i ->
                row.subMap(not_at) + at.collectEntries { k -> [(k) : row[k][i]] }
            }
        } as DataFrame
    }

    @Override
    AbstractDataFrame unnest_all() {
        return null
    }

    DataFrame slice(int... rows){
        slice(rows as ArrayList)
    }

    DataFrame slice(IntRange rows){
        slice(rows as ArrayList)
    }

    DataFrame slice(ArrayList rows){
        if (! allAreType(rows, Integer))
            throw new IllegalTypeException(
                errMsg("slice", "rows must be a list of integers.\n" +
                    "rows: $rows"))

        if (rows.any { it < 0 })
            throw new IndexOutOfBoundsException(
                errMsg("slice", "rows must all be positive .\n" +
                    "rows: $rows"))

        if (rows.max() >= nrow())
            throw new IndexOutOfBoundsException(
                errMsg("slice", "row index out of bounds.\n" +
                    "rows: $rows, ncol: ${nrow()}"))

        rows.collect { data[it as int] } as DataFrame

    }

    @Override
    String toString() {

        if (data.size() < 6)
            "${this.getClass().simpleName} (${nrow()} x ${ncol()}): [" + data.join(', ') + ']'
        else
            "${this.getClass().simpleName} (${nrow()} x ${ncol()}): [" + data.subList(0, 5).join(', ') + ', [... ]]'
    }

    TransposedDataFrame transpose() {
        (colNames.collectEntries { k ->
            [(k): data.collect { it[k] }]
        } as LinkedHashMap) as TransposedDataFrame
    }
}
