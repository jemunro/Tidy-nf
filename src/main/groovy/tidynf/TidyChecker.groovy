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

    static void checkLinkedHashMap(String method, Object object){
        if (!(object instanceof LinkedHashMap)) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "Expected LinkedHashMap, got class: ${object.getClass()}, value: $object")
            )
        }
    }

    static List requireAsList(String method, Object object){
        if (object instanceof List){
            return object
        }
        else if (object instanceof LinkedHashMap) {
             return object.collect { it.value }
        }
        else {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "Expected List, got ${object.getClass()}")
            )
        }
    }

    static void checkSize(String method, Integer it_size, Integer target_size){
        if (it_size != target_size) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "Expected size $target_size, got $it_size")
            )
        }
    }

    static void checkEqualSizes(String method, List lists){
        if (lists.any { !(it instanceof List) }) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "Expected List, got ${lists.find{ !(it instanceof List) }.getClass()}")
            )
        }
        if (lists.any { it.size() != lists[0].size() }) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "Lists not equally sized")
            )
        }
    }

    static void checkUnique(String method, List list){
        if (list.unique().size() != list.size()) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "All entries must be unique")
            )
        }
    }


    static void checkKeysMatch(String method, List keys, List expected){
        if (keys != expected) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "keyset mismatch - expected $expected, got $keys")
            )
        }
    }

    static void checkKeysAreSubset(String method, List sub, List sup){
        if (sub.any { ! sup.contains(it) }) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "keys not present - ${sub.findAll({ ! sup.contains(it) })}")
            )
        }
    }

    static void checkNoOverlap(String method, List set_a, List set_b){
        if (set_a.any { set_b.contains(it) }) {
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "Error - overlap in key sets")
            )
        }
    }

    static void checkContains(String method, Object key, List keys){
        if (!(keys.contains(key))){
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "key not present - $key")
            )
        }
    }

    static void checkContainsNot(String method, Object key, List keys){
        if (keys.contains(key)){
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "key already present - $key")
            )
        }
    }

    static void checkRequiredParams(String method, List required, Map params){
        if ( required.any { ! params.containsKey(it) }){
            throw new IllegalArgumentException(
                tidyErrorMsg(method, "required parameters ${required.findAll{! params.containsKey( it)}} not present")
            )
        }
    }

    static void checkParamTypes(String method, Map types, Map params) {
        params.forEach { k, v ->
            if (!types.containsKey(k)) {
                throw new IllegalArgumentException(
                    tidyErrorMsg(method, "got unexpected argument \"$k=$v\"")
                )
            }
            if (!types[k].isInstance(params[k])) {
                throw new IllegalArgumentException(
                    tidyErrorMsg(method, "for argument \"$k\" expected: ${types[k]}, got: ${params[k].getClass()}")
                )
            }
        }
    }

    static String tidyErrorMsg(String method, String error){
        "Tidy-nf ($method): $error"
    }

    static tidyError(String method, String error) {
        throw new Exception(
            tidyErrorMsg(method, error)
        )
    }
}
