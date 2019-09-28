package tidynf

import static tidynf.exception.TidyError.tidyError


class TidyChecks {


    static void checkIsType(Object object, Class type, String method_name){
        if (!(type.isInstance(object))) {
            tidyError("Expected class: $type, got class: ${object.getClass()}, offending value: $object.", method_name)
        }
    }

    static Boolean allAreType(Collection coll, Class type){
        coll.every { type.isInstance(it) }
    }

    static void checkAllAreType(List list, Class type, String method_name){
        if ( ! list.every { type.isInstance(it) } ) {
            tidyError( "Expected List of class: $type, got class: ${list.collect { it.getClass()} }" +
                " offending value: $list.", method_name)
        }
    }


    static void checkNonEmpty(Collection coll, String method_name){
        if (coll.size() < 1) {
            tidyError("empty List", method_name)
        }
    }


    static void checkEqualSizes(List lists, String method_name){
        if (lists.any { !(it instanceof Collection) }) {
            tidyError("Expected Collection, got ${lists.find{ !(it instanceof Collection) }.getClass()}", method_name)
        }
        if (lists.any { it.size() != lists[0].size() }) {
            tidyError("Lists not equally sized", method_name)
        }
    }

    static void checkEqualSizes(Collection col1, Collection col2, String method_name){
        if (col1.size() != col2.size()) {
            tidyError("Collections are not equally sized: ${col1.toString()}, ${col2.toString()}", method_name)
        }
    }

    static void checkUnique(List list, String method_name){
        if (list.unique().size() != list.size()) {
            tidyError("All entries must be unique", method_name)
        }
    }


    static void checkKeysMatch(LinkedHashSet expected, LinkedHashSet keys, String method_name){
        if (keys != expected) {
            tidyError("keyset mismatch - expected $expected, got $keys", method_name)
        }
    }


    static void checkNoOverlap(Collection set_a, Collection set_b, String method_name){
        if (set_a.any { set_b.contains(it) }) {
            tidyError("Error: sets must not overlap - ${set_a.toString()}, ${set_b.toString()}", method_name)
        }
    }


    static void checkContains(Collection coll, Object item, String method_name){
        if (!(coll.contains(item))) {
            tidyError("${item.toString()} not in ${coll.toString()}", method_name)
        }
    }


    static void checkContainsNot(Collection coll, Object item, String method_name) {
        if ((coll.contains(item))) {
            tidyError("${item.toString()} in ${coll.toString()}", method_name)
        }
    }


    static void checkContainsAll(Collection superSet, Collection subSet, String method_name){
        if (!(superSet.containsAll(subSet))) {
            tidyError("key(s) not present - ${subSet.find { ! superSet.contains(it) }.toString()}", method_name)
        }
    }


    static void checkRequiredParams(String method_name, List required, Map params){
        if ( required.any { ! params?.containsKey(it) }){
            tidyError("required parameters ${required.findAll {! params?.containsKey(it)}} not present", method_name)
        }
    }


    static void checkParamTypes(String method_name, Map types, Map params) {
        params?.forEach { k, v ->
            if (!types.containsKey(k)) {
                tidyError("got unexpected argument \"$k=$v\"", method_name)
            }
            if (!types[k].isInstance(params[k])) {
                println "Error: \"for argument \"$k\" expected: ${types[k]}, got: ${params[k].getClass()}"
                tidyError("for argument \"$k\" expected: ${types[k]}, got: ${params[k].getClass()}", method_name)
            }
        }
    }
}
