package tidyflow.dataframe.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.dataframe.DataFrame
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.isType

abstract class Operator {

    protected String name
    protected Integer counter = 0
    protected Boolean debug = false
    protected LinkedHashSet colNames
    protected LinkedHashSet colTarget
    protected Map par
    protected DataflowChannel source

    abstract DataflowChannel apply()

    void failWithException(Class exception, String message) {
        throw exception.newInstance(errMsg(name, message)) as Throwable
    }

    void checkIsDataFrame(Object o){
        if (! isType(o, DataFrame))
            failWithException(IllegalTypeException,
                "Required a {${DataFrame.simpleName} but got ${o.getClass().simpleName} with value \"$o\" instead")
    }

    void checkColNames(DataFrame df){
        if (df.names() != colNames)
           failWithException(KeySetMismatchException,
               "Required matching colnames\nexpected: $colNames, got: ${df.names()}")
    }



}