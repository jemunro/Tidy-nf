package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue

import static tidynf.TidyChecks.checkContainsAll
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.TidyDataFlow.leftRightExclusive
import static tidynf.TidyDataFlow.withUniqueKeyData
import static tidynf.exception.TidyError.tidyError

class JoinOp {

    private String method_name
    private DataflowQueue left
    private DataflowQueue right
    private LinkedHashSet keySetBy
    private LinkedHashSet keySetLeft
    private LinkedHashSet keySetRight
    private LinkedHashSet keySetFinal
    private final static LinkedHashSet validMethods =  ["left_join", "right_join", "inner_join", "full_join"]


    JoinOp(String method_name, DataflowQueue left, DataflowQueue right, Collection by) {

        this.method_name = method_name
        this.left = left
        this.right = right
        this.keySetBy = by

        if (! validMethods.contains(method_name)) {
            tidyError("unknown join method: $method_name", "join")
        }
    }

    DataflowQueue apply() {

        def res
        switch(method_name) {
            case "left_join":
                res = combineByTuple().filter { it[1] }
                break

            case "right_join":
                res = combineByTuple().filter { it[2] }
                break

            case "inner_join":
                res = combineByTuple().filter { it[1] && it[2] }
                break

            default:
                res = combineByTuple()
        }

        res.map {

            def data = (it[0] + it[1] + it[2]) as LinkedHashMap

            keySetFinal.collectEntries { k -> [(k): data[k] ] }
        }
    }

    DataflowQueue combineByTuple() {

        def left_unique
        def left_queue
        def left_exc
        def right_unique
        def right_queue
        def right_exc

        (left_unique, left_queue) = withUniqueKeyData(left, keySetBy)
        (right_unique, right_queue) = withUniqueKeyData(right, keySetBy)

        (left_exc, right_exc) = leftRightExclusive(left_unique, right_unique)

        def left_side = prepareForCombine(left_queue, true).mix(right_exc.map { [ it, [:] ] })
        def right_side = prepareForCombine(right_queue, false).mix(left_exc.map { [ it, [:] ] })

        left_side.combine(right_side, by: 0).map { mapChecks(it as List); it }
    }

    DataflowQueue prepareForCombine(DataflowQueue source, Boolean is_left) {

        source.map {

            checkIsType(it, LinkedHashMap, method_name)
            def data = it as LinkedHashMap

            if (is_left){
                synchronized (this) {
                    if (! keySetLeft) {
                        keySetLeft = data.keySet()

                        if (keySetRight) {
                           initKeySets()
                        }
                    }
                }
                checkKeysMatch(keySetLeft, data.keySet() as LinkedHashSet, method_name)

                [data.subMap(keySetBy), data.subMap(keySetLeft - keySetBy)]

            } else {
                synchronized (this) {
                    if (! keySetRight) {
                        keySetRight = data.keySet()

                        if (keySetLeft) {
                            initKeySets()
                        }
                    }
                }
                checkKeysMatch(keySetRight, data.keySet() as LinkedHashSet, method_name)

                [data.subMap(keySetBy), data.subMap(keySetRight - keySetBy)]
            }
        }
    }

    void initKeySets() {
        if (keySetBy.size() < 1) {
            tidyError("by must not be empty\n\t" +
                "attmepted join of ${keySetRight.toString()} with ${keySetLeft.toString()}", method_name)
        }
        checkContainsAll(keySetRight, keySetBy, method_name)
        checkContainsAll(keySetLeft, keySetBy, method_name)
        keySetFinal = keySetBy + keySetLeft + keySetRight
    }

    void mapChecks(Collection coll) {
        if (coll.size() != 3) {
            tidyError("Something went wrong, size is ${coll.size()} instead of 3", method_name)
        }
        checkIsType(coll[0], LinkedHashMap, method_name)
        checkIsType(coll[1], LinkedHashMap, method_name)
        checkIsType(coll[2], LinkedHashMap, method_name)
    }

}
