package tidynf


import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import static channelextra.ChannelExtraOperators.*

class TidyDataFlow {

    static List getKeys(Object object) {
        object instanceof LinkedHashMap ? object.keySet() as List : null
    }

    static List getKeyTuple(Object object, List by) {
        object instanceof LinkedHashMap ?
            object.findAll { by.contains(it.key) }.collect { it.value } :
            []
    }

    static LinkedHashMap getKeyData(Object object, List by) {
        object instanceof LinkedHashMap ?
            by.collectEntries { k -> [ (k): object[k] ] } as LinkedHashMap:
            [:] as LinkedHashMap
    }

    static DataflowVariable withKeys(DataflowVariable var) {
        var.map { [ keys: getKeys(it), data: it ] }
    }

    static DataflowQueue withKeys(DataflowQueue queue) {
        mergeWithFirst(queue).map { f, d -> [ keys: getKeys(f), data: d ] }
    }

    static DataflowChannel[] withKeysLeftRight(DataflowChannel left, DataflowChannel right) {

        def left_keys
        def left_queue
        def right_keys
        def right_queue
        
        (left_keys, left_queue) = left.into(2)
        left_keys = left_keys.first().map { getKeys(it) }
        (right_keys, right_queue) = right.into(2)
        right_keys = right_keys.first().map { getKeys(it) }
        
        def left_merged = mergeWithFirst(left_queue)
            .map { f, d -> [ data:d, left_keys: getKeys(f) ] }
            .merge(right_keys) { lm, rk -> lm + [right_keys: rk]}

        def right_merged = mergeWithFirst(right_queue)
            .map { f, d -> [ data:d, right_keys: getKeys(f) ] }
            .merge(left_keys) { lm, rk -> lm + [left_keys: rk]}

        [ left_merged, right_merged ]
    }

    static DataflowChannel[] withUniqueKeyData(DataflowChannel source, List by) {

        def a
        def b
        (a, b) = source.into(2)

        [ a.map { getKeyData(it, by) }.unique() , b ]
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
