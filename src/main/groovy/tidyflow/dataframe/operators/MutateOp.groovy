package tidyflow.dataframe.operators

import groovyx.gpars.dataflow.DataflowChannel
import tidyflow.dataframe.DataFrame
import tidyflow.exception.IllegalTypeException
import tidyflow.exception.KeySetMismatchException

import static tidyflow.exception.Message.errMsg
import static tidyflow.helpers.Predicates.areSameSet
import static tidyflow.helpers.Predicates.isType

class MutateOp extends Operator {

    private Map with
    private Closure closure

    MutateOp(DataflowChannel source, Closure closure, Map with, boolean debug = false){

        this.name = 'mutate'
        this.source = source
        this.debug = debug
        this.with = with
        this.closure = closure
    }

    DataflowChannel apply() {

        source.map {

            counter++

            checkIsDataFrame(it)

            DataFrame df = (DataFrame)it

            synchronized (this) {
                if (! colNames) {
                    colNames = df.names()
                }
            }

            checkColNames(df)

            df.mutate_with(with, closure)
        }
    }
}
