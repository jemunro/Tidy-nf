package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue

import static tidynf.TidyChecks.checkIsSubset
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyChecks.requireAsLinkedHashMap
import static tidynf.TidyDataFlow.leftRightExclusive
import static tidynf.TidyDataFlow.withKeysLeftRight
import static tidynf.TidyDataFlow.withUniqueKeyData

class JoinOp {

    private String method_name
    private DataflowQueue left
    private DataflowQueue right
    private List by

    JoinOp(String method_name, DataflowQueue left, DataflowQueue right, List by) {

        this.method_name = method_name
        this.left = left
        this.right = right
        this.by = by

        assert ["left_join", "right_join", "inner_join", "full_join"].contains(method_name)
    }

    DataflowQueue apply() {
        switch(method_name) {

            case "left":
                combineByTuple()
                    .filter { it.contains_left }
                    .map { it.by + it.left_data + it.right_data }
                break

            case "right":
                combineByTuple()
                    .filter { it.contains_right }
                    .map { it.by + it.left_data + it.right_data }
                break

            case "inner":
                combineByTuple()
                    .filter { it.contains_right && it.contains_left }
                    .map { it.by + it.left_data + it.right_data }
                break

            default:
                combineByTuple()
                    .map { it.by + it.left_data + it.right_data }
        }
    }

    DataflowQueue combineByTuple() {

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

        def left_side = prepareForCombine(left_wkeys, true).mix(right_exc.map { [ it, null ] })
        def right_side = prepareForCombine(right_wkeys, false).mix(left_exc.map { [ it, null ] })

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

    DataflowQueue prepareForCombine(DataflowQueue source, Boolean is_left) {

        source.map {
            it.data = requireAsLinkedHashMap(method_name, it.data)
            if (is_left){
                checkKeysMatch(it.left_keys, it.data.keySet() as List, method_name)
                checkIsSubset(method_name, by, it.left_keys)
            } else {
                checkKeysMatch(it.right_keys, it.data.keySet() as List, method_name)
                checkIsSubset(method_name, by, it.right_keys)
            }
            def by_data = by.collectEntries { k -> [(k): it.data[k] ] }
            def payload = [:]
            payload.data = it.data.findAll { ! by.contains(it.key) }
            payload.left_keys = it.left_keys.findAll { ! by.contains(it) }
            payload.right_keys = it.right_keys.findAll { ! by.contains(it) }
            [ by_data, payload ]
        }
    }
}
