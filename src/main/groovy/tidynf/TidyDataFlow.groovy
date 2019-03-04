package tidynf


import groovyx.gpars.dataflow.DataflowChannel
import static channelextra.ChannelExtraOperators.*
import static tidynf.TidyChecker.*

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
            by.collectEntries { k -> [ (k): object[k] ] }as LinkedHashMap:
            [:] as LinkedHashMap
    }

    static DataflowChannel withKeys(DataflowChannel channel) {
        mergeWithFirst(channel).map { first, x -> [ keys: getKeys(first), data: x ] }
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

    static DataflowChannel prepareForJoin(DataflowChannel source, List by, Boolean is_left, String method = 'prepareForJoin') {

        source.map {
            it.data = requireAsLinkedHashMap(method, it.data)
            if (is_left){
                checkKeysMatch(method, it.data.keySet() as List, it.left_keys)
                checkKeysAreSubset(method, by, it.left_keys)
            } else {
                checkKeysMatch(method, it.data.keySet() as List, it.right_keys)
                checkKeysAreSubset(method, by, it.right_keys)
            }
            def by_data = by.collectEntries { k -> [(k): it.data[k] ] }
            def payload = [:]
            payload.data = it.data.findAll { ! by.contains(it.key) }
            payload.left_keys = it.left_keys.findAll { ! by.contains(it) }
            payload.right_keys = it.right_keys.findAll { ! by.contains(it) }
            [ by_data, payload ]
        }
    }

    static DataflowChannel preJoin(DataflowChannel left, DataflowChannel right, List by) {

        def method = 'preJoin'
        def left_unique
        def left_queue
        def left_exc
        def left_wkeys
        def right_unique
        def right_queue
        def right_exc
        def right_wkeys

        (left_unique, left_queue) = withUniqueKeyData(left, by)
        (right_unique, right_queue) = withUniqueKeyData(right, by)

        (left_exc, right_exc) = leftRightExclusive(left_unique, right_unique)
        (left_wkeys, right_wkeys) = withKeysLeftRight(left_queue, right_queue)

        def left_side = prepareForJoin(left_wkeys, by, true, method).mix(right_exc.map { [ it, null ] })
        def right_side = prepareForJoin(right_wkeys, by, false, method).mix(left_exc.map { [ it, null ] })

        left_side.combine(right_side, by:0).map {
            def payload = [ by: it[0] ]
            payload.contains_left = ! it[1].is(null)
            payload.contains_right = ! it[2].is(null)

            def left_keys = payload.contains_left ? it[1].left_keys : it[2].left_keys
            def right_keys = payload.contains_right ? it[2].right_keys : it[1].right_keys
            def overlaps = left_keys.findAll { k -> right_keys.contains(k) }

            left_keys = left_keys
                .collect { overlaps.contains(it) ? it + '_left' : it }
            right_keys = right_keys
                .collect { overlaps.contains(it) ? it + '_right' : it }

            payload.left_data = payload.contains_left ? (
                [ left_keys, it[1].data.values() as List ]
                    .transpose()
                    .collectEntries{ k, v -> [ (k):v ] } ) :
                ( left_keys.collectEntries { k -> [ (k) : null] } )

            payload.right_data = payload.contains_right ? (
                [ right_keys, it[2].data.values() as List ]
                    .transpose()
                    .collectEntries{ k, v -> [ (k):v ] } ) :
                ( right_keys.collectEntries { k -> [ (k) : null] } )

            payload
        }
    }
}
