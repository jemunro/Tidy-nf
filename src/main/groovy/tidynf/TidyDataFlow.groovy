package tidynf

import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel

import static nextflow.Channel.create

class TidyDataFlow {
    static DataflowReadChannel splitKeysAndData(DataflowReadChannel channel) {
        def keysChannel = create()
        def dataChannel = create()
        channel.split(keysChannel as DataflowWriteChannel, dataChannel as DataflowWriteChannel)
        dataChannel.merge(
            keysChannel.first().map { it instanceof LinkedHashMap ? it.keySet() as List : null },
            { d, k -> [keys: k, data: d] }
        )
    }
}
