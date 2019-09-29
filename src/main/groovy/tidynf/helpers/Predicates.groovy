package tidynf.helpers

class Predicates {

    static boolean isListOfMap(Object object) {
        object instanceof List && ! isEmpty(object) && object.every { it instanceof Map }
    }

    static boolean isEmpty(Collection coll) {
        coll.size() == 0
    }

    static boolean isType(Object obj, Class type) {
        type.isInstance(obj)
    }

    static boolean allAreType(Collection coll, Class type){
        coll.every { type.isInstance(it) }
    }

    static boolean allAreSameSize(Collection coll) {
        (! isEmpty(coll)) && allAreType(coll, Collection) && coll.drop(1).every { areSameSize(it, coll[0]) }
    }

    static boolean allAreUnique(Collection coll) {
        coll.size() == coll.unique().size()
    }

    static boolean allKeySetsMatch(Collection coll) {
        (! isEmpty(coll)) && isListOfMap(coll) && coll.drop(1).every { it.keySet() == coll[0].keySet() }
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
