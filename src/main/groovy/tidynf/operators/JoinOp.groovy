package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue

import static tidynf.helpers.TidyChecks.checkContainsAll
import static tidynf.helpers.TidyChecks.checkIsType
import static tidynf.helpers.TidyChecks.checkKeysMatch
import static tidynf.helpers.JoinHelpers.leftRightExclusive
import static tidynf.helpers.JoinHelpers.withUniqueKeyData
import static tidynf.exception.TidyError.tidyError

class JoinOp {

    private String methodName
    private DataflowQueue left
    private DataflowQueue right
    private LinkedHashSet keySetBy
    private LinkedHashSet keySetLeft
    private LinkedHashSet keySetRight
    private LinkedHashSet keySetFinal
    private final static LinkedHashSet validMethods =  ["left_join", "right_join", "inner_join", "full_join"]


    JoinOp(String methodName, DataflowQueue left, DataflowQueue right, Collection by) {

        this.methodName = methodName
        this.left = left
        this.right = right
        this.keySetBy = by

        if (! validMethods.contains(methodName)) {
            tidyError("unknown join method: $methodName", "join")
        }
    }

    DataflowQueue apply() {

        def res
        switch(methodName) {
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

            checkIsType(it, LinkedHashMap, methodName)
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
                checkKeysMatch(keySetLeft, data.keySet() as LinkedHashSet, methodName)

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
                checkKeysMatch(keySetRight, data.keySet() as LinkedHashSet, methodName)

                [data.subMap(keySetBy), data.subMap(keySetRight - keySetBy)]
            }
        }
    }

    void initKeySets() {
        if (keySetBy.size() < 1) {
            tidyError("by must not be empty\n\t" +
                "attmepted join of ${keySetRight.toString()} with ${keySetLeft.toString()}", methodName)
        }
        checkContainsAll(keySetRight, keySetBy, methodName)
        checkContainsAll(keySetLeft, keySetBy, methodName)
        keySetFinal = keySetBy + keySetLeft + keySetRight
    }

    void mapChecks(Collection coll) {
        if (coll.size() != 3) {
            tidyError("Something went wrong, size is ${coll.size()} instead of 3", methodName)
        }
        checkIsType(coll[0], LinkedHashMap, methodName)
        checkIsType(coll[1], LinkedHashMap, methodName)
        checkIsType(coll[2], LinkedHashMap, methodName)
    }

}
