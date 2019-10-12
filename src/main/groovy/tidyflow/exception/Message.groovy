
package tidyflow.exception

class Message {

    static String errMsg(String method, String error) {
        "Tidy-nf ($method): $error"
    }

    static String errMsg(String error) {
        "Tidy-nf: $error"
    }

    static tidyError(String error, String method) {
        throw new TidyException(errMsg(method, error))
    }
}
