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

    static DataflowReadChannel splitKeysAndDataCross(DataflowReadChannel source, DataflowReadChannel target) {

        def target_keys = create()
        def target_data = create()

        target.split(target_keys as DataflowWriteChannel, target_data as DataflowWriteChannel)

        target_data
            .merge(
                target_keys
                    .first()
                    .map { it instanceof LinkedHashMap ? it.keySet() as List : null },
                { td, tk ->  [target_data: td, target_keys: tk] } )
            .merge(
                source
                    .toList()
                    .map { it -> [source_data:it, source_keys: it[0] instanceof LinkedHashMap ? it[0].keySet() as List : null] },
                { t, s -> t + s })
    }

    static LinkedHashMap splitKeysAndDataJoin(DataflowReadChannel left, DataflowReadChannel right) {

        def left_keys = create()
        def left_data = create()
        def right_keys = create()
        def right_data = create()

        left.split(left_keys as DataflowWriteChannel, left_data as DataflowWriteChannel)
        right.split(right_keys as DataflowWriteChannel, right_data as DataflowWriteChannel)

        def both_keys = left_keys.first().map { it instanceof LinkedHashMap ? it.keySet() as List : null }
            .merge(right_keys.first().map { it instanceof LinkedHashMap ? it.keySet() as List : null },
                { l, r -> [left_keys:l, right_keys:r] })
        [
            left: left_data.merge(both_keys, { d, k -> [data:d, left_keys: k.left_keys, right_keys: k.right_keys ] }),
            right: right_data.merge(both_keys, { d, k -> [data:d, left_keys: k.left_keys, right_keys: k.right_keys ] })
        ]
    }
}
