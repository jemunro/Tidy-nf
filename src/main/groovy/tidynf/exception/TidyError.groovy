
package tidynf.exception

class TidyError {

    static String tidyErrorMsg(String method, String error){
        "Tidy-nf ($method): $error"
    }

    static error(String method, String error) {
        throw new TidyException(tidyErrorMsg(method, error))
    }
}
