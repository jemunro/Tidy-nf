package tidyflow.operators

import groovyx.gpars.dataflow.DataflowQueue
import tidyflow.exception.EmptySetException
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException
import tidyflow.exception.TidyException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.JoinHelpers.leftRightExclusive
import static tidyflow.helpers.JoinHelpers.withUniqueKeyData
import static tidyflow.helpers.Predicates.areSameSet
import static tidyflow.helpers.Predicates.isEmpty
import static tidyflow.helpers.Predicates.isListOfMap
import static tidyflow.helpers.Predicates.isType

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

        assert validMethods.contains(methodName)

        if (isEmpty(keySetBy))
            throw new EmptySetException(errMsg(methodName, "keyset by must not be empty"))
    }

    DataflowQueue apply() {

        def res

        switch(methodName) {
            case "left_join":
                res = doJoin().filter { it[1] }
                break

            case "right_join":
                res = doJoin().filter { it[2] }
                break

            case "inner_join":
                res = doJoin().filter { it[1] && it[2] }
                break

            default:
                res = doJoin()
        }

        res.map {

            def data = (it[0] + it[1] + it[2]) as LinkedHashMap

            keySetFinal.collectEntries { k -> [(k): data[k] ] }
        }
    }

    DataflowQueue doJoin() {

        def left_unique
        def left_queue
        def left_exc
        def right_unique
        def right_queue
        def right_exc

        (left_unique, left_queue) = withUniqueKeyData(left, keySetBy)
        (right_unique, right_queue) = withUniqueKeyData(right, keySetBy)

        (left_exc, right_exc) = leftRightExclusive(left_unique, right_unique)

        def left_side = prepareCombine(left_queue, true).mix(right_exc.map { [it, [:] ] })
        def right_side = prepareCombine(right_queue, false).mix(left_exc.map { [it, [:] ] })

        left_side.combine(right_side, by: 0).map {

            if (!isType(it, List))
                throw new IllegalTypeException(errMsg(methodName, "Required List type\n" +
                        "got ${it.getClass().simpleName} with value $it"))

            ArrayList list = it as ArrayList

            if (list.size() != 3)
                throw new TidyException(errMsg(methodName, "Something went wrong, size is ${list.size()} instead of 3"))

            if (!isListOfMap(list))
                throw new IllegalTypeException(errMsg("Required List of Map\ngot: $list"))

            list
        }
    }

    DataflowQueue prepareCombine(DataflowQueue source, Boolean is_left) {

        source.map {

            if (! isType(it, Map))
                throw new IllegalTypeException(errMsg(methodName, "Required Map type\n" +
                        "got ${it.getClass().simpleName} with value $it"))

            LinkedHashMap data = it as LinkedHashMap

            if (is_left){
                synchronized (this) {
                    if (! keySetLeft) {
                        keySetLeft = data.keySet()

                        if (keySetRight) {
                           initKeySets()
                        }
                    }
                }

                if (! areSameSet(keySetLeft, data.keySet()))
                    throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                            "\nfirst keyset: $keySetLeft\nmismatch keyset: ${data.keySet()}"))

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

                if (! areSameSet(keySetRight, data.keySet()))
                    throw new KeySetMismatchException(errMsg(methodName, "Required matching keysets" +
                            "\nfirst keyset: $keySetRight\nmismatch keyset: ${data.keySet()}"))

                [data.subMap(keySetBy), data.subMap(keySetRight - keySetBy)]
            }
        }
    }

    void initKeySets() {

        if (!keySetRight.containsAll(keySetBy))
            throw new KeySetMismatchException(errMsg(methodName, "keyset right does not contain all of keyset by\n" +
                    "keyset by: $keySetBy, keyset right: $keySetRight"))

        if (!keySetLeft.containsAll(keySetBy))
            throw new KeySetMismatchException(errMsg(methodName, "keyset left does not contain all of keyset by\n" +
                    "keyset by: $keySetBy, keyset right: $keySetLeft"))

        keySetFinal = keySetBy + keySetLeft + keySetRight
    }

}
