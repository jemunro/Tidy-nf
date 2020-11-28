package channelextra

import groovy.json.JsonOutput
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.DataflowVariable


import java.nio.file.Path

class ChannelExtraOperators {

    static DataflowWriteChannel[] withFirst (DataflowQueue source){
        def q1
        def q2
        (q1, q2) = source.into(2)
        def first = q1.first()
        return [ first,  q2 ]
    }

    static DataflowVariable[] withFirst (DataflowVariable source) {
        return [ source, source ]
    }

    static DataflowQueue mergeWithFirst (DataflowQueue source){
        def first
        def each
        (first, each) = withFirst(source)
        each.merge(first) { e, f -> [f, e] }
    }

    static DataflowVariable mergeWithFirst (DataflowVariable source){
        source.map { [ it, it ] }
    }

    static DataflowWriteChannel toTransList (DataflowWriteChannel channel, sort = true) {
        if (sort){
            channel.toSortedList().map { it.transpose() }
        } else {
            channel.toList().map { it.transpose() }
        }
    }

    static DataflowWriteChannel plusFileSize (DataflowWriteChannel channel, String units='GB') {
        units = units.toUpperCase()
        def div = (units == 'GB' ? 1e9 : (units == 'MB' ? 1e6 : (units == 'KB' ? 1e3 : 1)))
        channel.map {
            it = (it instanceof List ? it : [it])
            it + it.collectNested { it instanceof Path ? it.toFile().length() / div : 0 }.flatten().sum()
        }
    }

    static DataflowWriteChannel fileSizeSort (DataflowWriteChannel channel){
        from(
            channel.toList().get()
                .collect { it instanceof List ?
                it + it.collectNested { iit -> iit instanceof Path ? iit.toFile().length() / 1e9 : 0 }.flatten().sum() :
                it instanceof Path ? [it, it.toFile().length() / 1e9] : [it, 0] }
            .toSorted { a, b -> b.last() <=> a.last() }
                .collect { it.dropRight(1) } )
    }

    static DataflowWriteChannel toJson(DataflowQueue channel, Path path) {
        toJson(channel, path.toFile())
    }

    static DataflowWriteChannel toJson(DataflowQueue channel, String path) {
        toJson(channel, (new File(path)))
    }

    static DataflowWriteChannel toJson(DataflowQueue channel, File json) {
        def parent = json.toPath().toAbsolutePath().parent
        if (! parent.exists()) { parent.mkdirs() }
        def list = channel.toList().get().collectNested { it ->
            it instanceof java.nio.file.Path ? it.toRealPath().toAbsolutePath().toUri().toString() : it
        }
        json.write(JsonOutput.toJson(list) + '\n', 'utf-8')
        return from(json.toPath()).first()
    }

    static DataflowWriteChannel toTsv(DataflowQueue channel, String filename) {
        toDelim(channel, (new File(filename)), '\t', [])
    }

    static DataflowWriteChannel toCsv(DataflowQueue channel, String filename) {
        toDelim(channel, (new File(filename)), ',', [])
    }

    static DataflowWriteChannel toTsv(DataflowQueue channel, String filename, List colNames) {
        toDelim(channel, (new File(filename)), '\t', colNames)
    }

    static DataflowWriteChannel toCsv(DataflowQueue channel, String filename, List colNames) {
        toDelim(channel, (new File(filename)), ',', colNames)
    }

    static DataflowWriteChannel toDelim(DataflowQueue channel, File file, String delim, List colNames) {
        def parent = file.toPath().toAbsolutePath().toFile().parentFile
        if (! parent.exists()) { parent.mkdirs() }
        if (colNames) {
            file.write(colNames.join(delim) + '\n', 'utf-8')
        } else {
            file.write('', 'utf-8')
        }
        channel.toSortedList().map {
            file.append(it.collect {
                it.collect {
                    it instanceof Path ? it.toAbsolutePath().toString() : it.toString() }
                .join(delim) }
            .join('\n') + '\n', 'utf-8')
            file.toPath()
        }
    }

    static void subscribeToTsv(DataflowWriteChannel channel, file) {
        subscribeToDelim(channel, file, '\t' )
    }

    static void subscribeToCsv(DataflowWriteChannel channel, file) {
        subscribeToDelim(channel, file, ',')
    }

    static void subscribeToDelim(DataflowWriteChannel channel, String file, String delim){
        subscribeToDelim(channel, (new File(file)), delim )
    }

    static void subscribeToDelim(DataflowWriteChannel channel, Path path, String delim){
        subscribeToDelim(channel, path.toFile(), delim )
    }

    static void subscribeToDelim(DataflowWriteChannel channel, File file, String delim='\t'){
        def parent = file.toPath().toAbsolutePath().toFile().parentFile
        if (! parent.exists()) { parent.mkdirs() }
        file.write('', 'utf-8')
        channel.subscribe {
            def values = ( it instanceof LinkedHashMap ? it.values() : it )
                .collect { it instanceof Path ? it.toRealPath().toAbsolutePath().toString() : it }
            file.append(values.join(delim) + '\n', 'utf-8')
        }
    }

    static DataflowWriteChannel sortTuplesBy (DataflowWriteChannel channel, Integer by, rev = false){
        channel.map { it ->
            def at = it.findIndexValues { it instanceof List ? it.size() > 1 : false }.collect { it as Integer }
            by = at.indexOf(by)
            def max_size = it[at].collect { it.size() }.max()
            it[at] = it[at]
                .collect { it.size() < max_size ? it + [null] * (max_size - it.size()) : it }
                .transpose().sort({ a, b -> a[by] <=> b[by] }).transpose()
                .findAll { ! it.is(null)}
            if (rev) {
                it[at] = it[at].collect{ it.reverse() }
            }
            it
        }
    }
}