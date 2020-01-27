package tidyflow.helpers

import tidyflow.dataframe.operators.CollectColsOp

class Predicates {

    static boolean isListOfMap(Object object) {
        object instanceof Collection && !isEmpty(object) && object.every { it instanceof Map }
    }

    static boolean isListOfList(Object object) {
        object instanceof Collection && !isEmpty(object) && object.every { it instanceof List }
    }

    static boolean isMapOfList(Object object) {
        object instanceof Map && !isEmpty(object) && object.values().every { it instanceof List }
    }

    static boolean isEmpty(Collection coll) {
        coll.size() == 0
    }

    static boolean isEmpty(Map map) {
        map.size() == 0
    }

    static boolean isType(Object obj, Class type) {
        type.isInstance(obj)
    }

    static boolean allAreType(Collection coll, Class type) {
        coll.every { type.isInstance(it) }
    }

    static boolean allAreSameSize(Collection coll) {
        (!isEmpty(coll)) && allAreType(coll, Collection) &&
                coll.drop(1).every { areSameSize(it as Collection, coll[0] as Collection) }
    }

    static boolean allAllAreSameSize(Collection coll) {
        coll.every { allAreSameSize(it as Collection) }
    }

    static boolean allKeySetsMatch(Collection coll) {
        (!isEmpty(coll)) && isListOfMap(coll) && coll.drop(1).every { it.keySet() == coll[0].keySet() }
    }

    static boolean isListOfMapOfSameType(Collection coll) {
        if (allKeySetsMatch(coll)) {
            LinkedHashMap types = [:]
            return coll.collect {
                it.collect {
                    k, v ->
                        v == null ?: types[k] == null ?
                                { types[k] = v.getClass(); true }() :
                                types[k] == v.getClass()
                }.every()
            }.every()
        }
        false
    }

    static boolean isListOfSameType(Collection coll) {
        Class type = null
        coll.collect { item ->
            item == null ?: type == null ?
                    { type = item.getClass(); true }() :
                    type == item.getClass()
        }.every()
    }

    static boolean allAreListOfSameType(Collection coll) {
        isListOfList(coll) && coll.every { isListOfSameType(it as Collection) }
    }

    static boolean allAreListOfSameType(Map map) {
        allAreListOfSameType(map.values())
    }

    static boolean allKeySetsSameOrder(Collection coll) {
        allKeySetsMatch(coll) && coll.drop(1).every { (it.keySet() as ArrayList) == (coll[0].keySet() as ArrayList) }
    }

    static boolean areSameSet(Collection c1, Collection c2) {
        (c1 as Set) == (c2 as Set)
    }

    static boolean areSameSet(Set s1, Set s2) {
        s1 == s2
    }

    static boolean areSameSize(Collection c1, Collection c2) {
        c1.size() == c2.size()
    }
}
