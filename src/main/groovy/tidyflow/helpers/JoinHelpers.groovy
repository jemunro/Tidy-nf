package tidyflow.helpers


import groovyx.gpars.dataflow.DataflowChannel

class JoinHelpers {

    static DataflowChannel[] withUniqueKeyData(DataflowChannel source, Set by) {

        def a
        def b
        (a, b) = source.into(2)

        [ a.map { (it as LinkedHashMap).subMap(by) }.unique() , b ]
    }

    static DataflowChannel[] leftRightExclusive(DataflowChannel left, DataflowChannel right) {

        def left_exclusive
        def right_exclusive

        (left_exclusive, right_exclusive) = left
            .map { [it, true] }.join( right.map { [it, true] }, by:0, remainder:true)
            .into(2)

        left_exclusive = left_exclusive
            .filter { it[1] && (!(it[2]) )}
            .map { it[0] }

        right_exclusive = right_exclusive
            .filter { (!it[1]) && it[2] }
            .map { it[0] }

        [ left_exclusive, right_exclusive ]
    }
}
