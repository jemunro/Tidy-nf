package tidynf.helpers

class TidyHelpers {

    static List coerceToList(Object object, String methodName){
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
}