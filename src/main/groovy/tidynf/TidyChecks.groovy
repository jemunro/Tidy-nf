package tidynf

import static tidynf.TidyHelpers.keySetList
import static tidynf.exception.TidyError.error


class TidyChecks {

    static LinkedHashMap requireAsLinkedHashMap(String method, Object object){
        if (!(object instanceof LinkedHashMap)) {
            error("Expected LinkedHashMap, got class: ${object.getClass()}, value: $object\"", method)
        }
        object as LinkedHashMap
    }

    static void checkIsType(Object object, Class type, String method_name){
        if (!(type.isInstance(object))) {
            error("Expected class: $type, got class: ${object.getClass()}, offending value: $object.", method_name)
        }
    }

    static void checkAllAreType(List list, Class type, String method_name){
        if ( ! list.every { type.isInstance(it) } ) {
            error( "Expected List of class: $type, got class: ${list.collect { it.getClass()} }" +
                " offending value: $list.", method_name)
        }
    }

    static void checkIsLinkedHashMap(String method_name, Object object){
        if (!(object instanceof LinkedHashMap)) {
            error("Expected LinkedHashMap, got class: ${object.getClass()}, value: $object", method_name)
        }
    }

    static boolean hasKeys(LinkedHashMap map, List keys){
        isSubset(keys, keySetList(map))
    }

    static void checkHasKeys(LinkedHashMap map, List keys, String method_name){
        if (! hasKeys(map, keys)) {
            error( "Keyset: ${keySetList(map)} does not contain keys: $keys", method_name)
        }
    }

    static void checkHasKey(LinkedHashMap map, String key, String method_name){
        if (! map.containsKey(key)) {
            error("Keyset: ${keySetList(map)} does not contain key: $key", method_name)
        }
    }

    static boolean isNonEmpty(List list){
        list.size() > 0
    }

    static void checkNonEmpty(List list, String method_name){
        if (! isNonEmpty(list)) {
            error("empty List", method_name)
        }
    }



    static List requireAsList(Object object, String method_name){
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
                error("Expected size ${names.size()}, got ${object.size()}. " +
                    "Offending value: $object, names: $names", method_name)
            }
        } else {
            error("Unexpected error: couldn't check size for class: ${object.getClass()}, value: $object", method_name)
        }
    }

    static void checkEqualSizes(List lists, String method_name){
        if (lists.any { !(it instanceof List) }) {
            error("Expected List, got ${lists.find{ !(it instanceof List) }.getClass()}", method_name)
        }
        if (lists.any { it.size() != lists[0].size() }) {
            error("Lists not equally sized", method_name)
        }
    }

    static void checkUnique(List list, String method_name){
        if (list.unique().size() != list.size()) {
            error("All entries must be unique", method_name)
        }
    }


    static void checkKeysMatch(List expected, List keys, String method_name){
        if (keys != expected) {
            error("keyset mismatch - expected $expected, got $keys", method_name)
        }
    }

    static boolean  isSubset(List list_a, List list_b){
        return  ( list_a.every { list_b.contains(it) } )
    }

    static void checkIsSubset(String method_name, List sub, List sup){
        if (! isSubset(sub, sup)) {
            error("keys not present - ${sub.findAll({ ! sup.contains(it) })}", method_name)
        }
    }

    static void checkNoOverlap(String method_name, List set_a, List set_b){
        if (set_a.any { set_b.contains(it) }) {
            error("Error - overlap in key sets", method_name)
        }
    }

    static void checkContains(String method_name, Object key, List keys){
        if (!(keys.contains(key))) {
            error("key not present - $key", method_name)
        }
    }

    static void checkContainsNot(List keys, Object key, String method_name){
        if (keys.contains(key)){
            error("key already present - $key", method_name)
        }
    }

    static void checkRequiredParams(String method_name, List required, Map params){
        if ( required.any { ! params?.containsKey(it) }){
            error("required parameters ${required.findAll {! params?.containsKey(it)}} not present", method_name)
        }
    }

    static void checkParamTypes(String method_name, Map types, Map params) {
        params?.forEach { k, v ->
            if (!types.containsKey(k)) {
                error("got unexpected argument \"$k=$v\"", method_name)
            }
            if (!types[k].isInstance(params[k])) {
                /*
                    To-do - figure out why this is thrown as "Unexpected error [UndeclaredThrowableException]"
                 */
                println "Error: \"for argument \"$k\" expected: ${types[k]}, got: ${params[k].getClass()}"
                error("for argument \"$k\" expected: ${types[k]}, got: ${params[k].getClass()}", method_name)
            }
        }
    }
}
