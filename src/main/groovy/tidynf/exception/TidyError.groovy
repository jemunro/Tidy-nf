
package tidynf.exception

class TidyError {

    static String tidyErrorMsg(String error, String method){
        "Tidy-nf ($method): $error"
    }

    static error(String error, String method) {
        throw new TidyException(tidyErrorMsg(error, method))
    }
}
