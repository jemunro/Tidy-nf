package tidynf


class TidyChecker {

    static LinkedHashMap requireAsLinkedHashMap(String method, Object object){
        if (!(object instanceof LinkedHashMap)) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "Expected LinkedHashMap, got class: ${object.getClass()}, value: $object")
            )
        }
        object
    }

    static void checkIsType(Object object, Class type, String method_name){
        if (!(type.isInstance(object))) {
            tidyError(method_name, "Expected class: $type, got class: ${object.getClass()}, offending value: $object.")
        }
    }

    static void checkAllAreType(List list, Class type, String method_name){
        if ( ! list.every { type.isInstance(it) } ) {
            tidyError(method_name, "Expected List of class: $type, got class: ${list.collect { it.getClass()} }" +
                " offending value: $list.")
        }
    }

    static void checkIsLinkedHashMap(String method_name, Object object){
        if (!(object instanceof LinkedHashMap)) {
            tidyError(method_name, "Expected LinkedHashMap, got class: ${object.getClass()}, value: $object")
        }
    }

    static boolean hasKeys(LinkedHashMap map, List keys){
        isSubset(keys, map.keySet() as List)
    }

    static void checkHasKeys(LinkedHashMap map, List keys, String method_name){
        if (! hasKeys(map, keys)) {
            tidyError(method_name, "Keyset: ${map.keySet() as List} does not contain keys: $keys")
        }
    }

    static void checkHasKey(LinkedHashMap map, String key, String method_name){
        if (! map.containsKey(key)) {
            tidyError(method_name, "Keyset: ${map.keySet() as List} does not contain key: $key")
        }
    }

    static boolean isNonEmpty(List list){
        list.size() > 0
    }

    static void checkNonEmpty(List list, String method_name){
        if (! isNonEmpty(list)) {
            tidyError(method_name, "empty List")
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
                throw new IllegalArgumentException(
                    tidyErrorMsg(method_name, "Expected size ${names.size()}, got ${object.size()}. " +
                        "Offending value: $object, names: $names")
                )
            }
        } else {
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "Unexpected error: couldn't check size for class: ${object.getClass()}, value: $object")
            )
        }
    }

    static void checkEqualSizes(List lists, String method_name){
        if (lists.any { !(it instanceof List) }) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "Expected List, got ${lists.find{ !(it instanceof List) }.getClass()}")
            )
        }
        if (lists.any { it.size() != lists[0].size() }) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "Lists not equally sized")
            )
        }
    }

    static void checkUnique(List list, String method_name){
        if (list.unique().size() != list.size()) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "All entries must be unique")
            )
        }
    }


    static void checkKeysMatch(List expected, List keys, String method_name){
        if (keys != expected) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "keyset mismatch - expected $expected, got $keys")
            )
        }
    }

    static boolean  isSubset(List list_a, List list_b){
        return  ( list_a.every { list_b.contains(it) } )
    }

    static void checkIsSubset(String method_name, List sub, List sup){
        if (! isSubset(sub, sup)) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "keys not present - ${sub.findAll({ ! sup.contains(it) })}")
            )
        }
    }

    static void checkNoOverlap(String method_name, List set_a, List set_b){
        if (set_a.any { set_b.contains(it) }) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "Error - overlap in key sets")
            )
        }
    }

    static void checkContains(String method_name, Object key, List keys){
        if (!(keys.contains(key))){
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "key not present - $key")
            )
        }
    }

    static void checkContainsNot(String method_name, Object key, List keys){
        if (keys.contains(key)){
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "key already present - $key")
            )
        }
    }

    static void checkRequiredParams(String method_name, List required, Map params){
        if ( required.any { ! params.containsKey(it) }){
            throw new IllegalArgumentException(
                tidyErrorMsg(method_name, "required parameters ${required.findAll{! params.containsKey( it)}} not present")
            )
        }
    }

    static void checkParamTypes(String method_name, Map types, Map params) {
        params.forEach { k, v ->
            if (!types.containsKey(k)) {
                throw new IllegalArgumentException(
                    tidyErrorMsg(method_name, "got unexpected argument \"$k=$v\"")
                )
            }
            if (!types[k].isInstance(params[k])) {
                throw new IllegalArgumentException(
                    tidyErrorMsg(method_name, "for argument \"$k\" expected: ${types[k]}, got: ${params[k].getClass()}")
                )
            }
        }
    }

    static String tidyErrorMsg(String method_name, String error){
        "Tidy-nf ($method_name): $error"
    }

    static tidyError(String method_name, String error) {
        throw new Exception(
            tidyErrorMsg(method_name, error)
        )
    }
}
