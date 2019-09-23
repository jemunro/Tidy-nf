
package tidynf.exception

class TidyError {

    static String tidyErrorMsg(String error, String method){
        "Tidy-nf ($method): $error"
    }

    static tidyError(String error, String method) {
        throw new TidyException(tidyErrorMsg(error, method))
    }
}
