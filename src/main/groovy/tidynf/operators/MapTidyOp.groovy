package tidynf.operators

import groovyx.gpars.dataflow.DataflowChannel

import static tidynf.exception.TidyError.error

class MapTidyOp {

    static DataflowChannel mapTidyOp(String method_name, DataflowChannel channel, Closure closure) {

        withKeys(channel).map {
            it.data = requireAsLinkedHashMap(method_name, it.data)
            checkKeysMatch(method_name, it.data.keySet() as List, it.keys as List)
            closure(it.data)
        }
    }
}