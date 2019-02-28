package tidynf


import groovyx.gpars.dataflow.DataflowChannel
import static channelextra.ChannelExtraOperators.*
import static tidynf.TidyChecker.*
import static nextflow.Channel.create

class TidyDataFlow {

    static List getKeys(Object object) {
        object instanceof LinkedHashMap ? object.keySet() as List : null
    }

    static List getKeyTuple(Object object, List by) {
        object instanceof LinkedHashMap ?
            object.findAll { by.contains(it.key) }.collect { it.value } :
            []
    }

    static DataflowChannel withKeys(DataflowChannel channel) {
        mergeWithFirst(channel).map { first, x -> [ keys: getKeys(first), data: x ] }
    }

    static LinkedHashMap withKeysLeftRight(DataflowChannel left, DataflowChannel right) {

        def left_first
        def left_queue
        def right_first
        def right_queue

        (left_first, left_queue) = withFirst(left)
        (right_first, right_queue) = withFirst(right)

        def keys = left_first.map { getKeys(it) }
            .merge(right_first.map{ getKeys(it) },
            { l, r -> [ left_keys:l, right_keys:r ] } )
        [
            left: left_queue.merge(keys, { d, k -> [ data:d ] + k }),
            right: right_queue.merge(keys, { d, k -> [ data:d ] + k }),
        ]

    }

    static DataflowChannel[] withUniqueKeyTuples(DataflowChannel source, List by) {

        def a
        def b
        (a, b) = source.into(2)

        [ a.map { getKeyTuple(it, by) }.unique() , b ]
    }

    static DataflowChannel[] leftRightExclusive(DataflowChannel left, DataflowChannel right) {

        def left_exclusive = create()
        def right_exclusive = create()
        left.map { [it, true] }.join( right.map { [it, true] }, by:0, remainder:true)
            .filter { (!it[1]) || (!it[2]) }
            .choice (left_exclusive, right_exclusive) { it[1] ? 0 : 1 }

        [ left_exclusive.map { it[0] }, right_exclusive.map { it[0] } ]
    }

    static DataflowChannel leftRightCombineBy(DataflowChannel left, DataflowChannel right, List by){
        leftRightCombineBy('leftRightCombineBy', left, right, by)
    }

    static DataflowChannel leftRightCombineBy(String method, DataflowChannel left, DataflowChannel right, List by) {

        def left_unique
        def left_queue
        def right_unique
        def right_queue
        def left_exc
        def right_exc
        (left_unique, left_queue) = withUniqueKeyTuples(left, by)
        (right_unique, right_queue) = withUniqueKeyTuples(right, by)

        (left_exc, right_exc) = leftRightExclusive(left_unique, right_unique)
        def split = withKeysLeftRight(left_queue, right_queue)

        split.left.map {
            it.data = requireAsLinkedHashMap(method, it.data)
            checkKeysMatch(method, it.data.keySet() as List, it.left_keys)
            checkKeysAreSubset(method, by, it.left_keys)
            [ by.collect { k -> it.data[k] }, it.findAll { ! by.contains(it.key) } ] }
        .mix(right_exc.map { [ it, null ] })
            .combine(
            split.right.map {
                it.data = requireAsLinkedHashMap(method, it.data)
                checkKeysMatch(method, it.data.keySet() as List, it.right_keys)
                checkKeysAreSubset(method, by, it.right_keys)
                [ by.collect { k -> it.data[k] }, it.findAll { ! by.contains(it.key) } ] }
            .mix(left_exc.map { [ it, null ] }),
            by: 0)
    }
}
