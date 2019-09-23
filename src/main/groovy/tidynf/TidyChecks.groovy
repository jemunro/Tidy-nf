package tidynf

import static tidynf.TidyHelpers.keySetList
import static tidynf.exception.TidyError.tidyError


class TidyChecks {

    static LinkedHashMap requireAsLinkedHashMap(String method, Object object){
        if (!(object instanceof LinkedHashMap)) {
            tidyError("Expected LinkedHashMap, got class: ${object.getClass()}, value: $object\"", method)
        }
        object as LinkedHashMap
    }

    static void checkIsType(Object object, Class type, String method_name){
        if (!(type.isInstance(object))) {
            tidyError("Expected class: $type, got class: ${object.getClass()}, offending value: $object.", method_name)
        }
    }

    static void checkAllAreType(List list, Class type, String method_name){
        if ( ! list.every { type.isInstance(it) } ) {
            tidyError( "Expected List of class: $type, got class: ${list.collect { it.getClass()} }" +
                " offending value: $list.", method_name)
        }
    }

    static void checkIsLinkedHashMap(String method_name, Object object){
        if (!(object instanceof LinkedHashMap)) {
            tidyError("Expected LinkedHashMap, got class: ${object.getClass()}, value: $object", method_name)
        }
    }

    static boolean hasKeys(LinkedHashMap map, List keys){
        isSubset(keys, keySetList(map))
    }

    static void checkHasKeys(LinkedHashMap map, List keys, String method_name){
        if (! hasKeys(map, keys)) {
            tidyError( "Keyset: ${keySetList(map)} does not contain keys: $keys", method_name)
        }
    }

    static void checkHasKey(LinkedHashMap map, String key, String method_name){
        if (! map.containsKey(key)) {
            tidyError("Keyset: ${keySetList(map)} does not contain key: $key", method_name)
        }
    }

    static void checkNonEmpty(Collection coll, String method_name){
        if (coll.size() < 1) {
            tidyError("empty List", method_name)
        }
    }



    static List coerceToList(Object object, String method_name){
        if (object instanceof List){
            return object
        }
        else if (object instanceof LinkedHashMap) {
            return object.collect { it.value }
        }
        else {
            return [ object ]
        }
    }

    static void checkSetNames(List names, Object object, String method_name) {
        if (object.getMetaClass().respondsTo(object, 'size')) {
            if (object.size() != names.size()) {
                tidyError("Expected size ${names.size()}, got ${object.size()}. " +
                    "Offending value: $object, names: $names", method_name)
            }
        } else {
            tidyError("Unexpected tidyError: couldn't check size for class: ${object.getClass()}, value: $object", method_name)
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

    static boolean  isSubset(List list_a, List list_b){
        return  ( list_a.every { list_b.contains(it) } )
    }

    static void checkIsSubset(String method_name, List sub, List sup){
        if (! isSubset(sub, sup)) {
            tidyError("keys not present - ${sub.findAll({ ! sup.contains(it) })}", method_name)
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
