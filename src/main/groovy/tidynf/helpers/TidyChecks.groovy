package tidynf.helpers

import static tidynf.exception.TidyError.tidyError


class TidyChecks {


    static void checkIsType(Object object, Class type, String methodName){
        if (!(type.isInstance(object))) {
            tidyError("Expected class: $type, got class: ${object.getClass()}, offending value: $object.", methodName)
        }
    }

    static Boolean allAreType(Collection coll, Class type){
        coll.every { type.isInstance(it) }
    }

    static void checkAllAreType(List list, Class type, String methodName){
        if ( ! list.every { type.isInstance(it) } ) {
            tidyError( "Expected List of class: $type, got class: ${list.collect { it.getClass()} }" +
                " offending value: $list.", methodName)
        }
    }


    static void checkNonEmpty(Collection coll, String methodName){
        if (coll.size() < 1) {
            tidyError("empty List", methodName)
        }
    }


    static void checkEqualSizes(List lists, String methodName){
        if (lists.any { !(it instanceof Collection) }) {
            tidyError("Expected Collection, got ${lists.find{ !(it instanceof Collection) }.getClass()}", methodName)
        }
        if (lists.any { it.size() != lists[0].size() }) {
            tidyError("Lists not equally sized", methodName)
        }
    }

    static void checkEqualSizes(Collection col1, Collection col2, String methodName){
        if (col1.size() != col2.size()) {
            tidyError("Collections are not equally sized: ${col1.toString()}, ${col2.toString()}", methodName)
        }
    }

    static void checkUnique(List list, String methodName){
        if (list.unique().size() != list.size()) {
            tidyError("All entries must be unique", methodName)
        }
    }


    static void checkKeysMatch(LinkedHashSet expected, LinkedHashSet keys, String methodName){
        if (keys != expected) {
            tidyError("keyset mismatch - expected $expected, got $keys", methodName)
        }
    }


    static void checkNoOverlap(Collection set_a, Collection set_b, String methodName){
        if (set_a.any { set_b.contains(it) }) {
            tidyError("Error: sets must not overlap - ${set_a.toString()}, ${set_b.toString()}", methodName)
        }
    }


    static void checkContains(Collection coll, Object item, String methodName){
        if (!(coll.contains(item))) {
            tidyError("${item.toString()} not in ${coll.toString()}", methodName)
        }
    }


    static void checkContainsNot(Collection coll, Object item, String methodName) {
        if ((coll.contains(item))) {
            tidyError("${item.toString()} in ${coll.toString()}", methodName)
        }
    }


    static void checkContainsAll(Collection superSet, Collection subSet, String methodName){
        if (!(superSet.containsAll(subSet))) {
            tidyError("key(s) not present - ${subSet.find { ! superSet.contains(it) }.toString()}", methodName)
        }
    }


    static void checkRequiredParams(String methodName, List required, Map params){
        if ( required.any { ! params?.containsKey(it) }){
            tidyError("required parameters ${required.findAll {! params?.containsKey(it)}} not present", methodName)
        }
    }


    static void checkParamTypes(String methodName, Map types, Map params) {
        params?.forEach { k, v ->
            if (!types.containsKey(k)) {
                tidyError("got unexpected argument \"$k=$v\"", methodName)
            }
            if (!types[k].isInstance(params[k])) {
                println "Error: \"for argument \"$k\" expected: ${types[k]}, got: ${params[k].getClass()}"
                tidyError("for argument \"$k\" expected: ${types[k]}, got: ${params[k].getClass()}", methodName)
            }
        }
    }
}
