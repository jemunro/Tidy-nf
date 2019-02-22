package channelextra

import groovy.json.JsonOutput
import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue

import java.nio.file.Path

class ChannelExtraOperators {
    /* -------------------- Operators to add to Channels  -------------------- */
    //fix this to check Lists all contain strings

    static toTransList (DataflowChannel channel, sort = true) {
        if (sort){
            channel.toSortedList().map { it.transpose() }
        } else {
            channel.toList().map { it.transpose() }
        }
    }

    static plusFileSize (DataflowChannel channel, String units='GB') {
        units = units.toUpperCase()
        def div = (units == 'GB' ? 1e9 : (units == 'MB' ? 1e6 : (units == 'KB' ? 1e3 : 1)))
        channel.map {
            it = (it instanceof List ? it : [it])
            it + it.collectNested { it instanceof Path ? it.toFile().length() / div : 0 }.flatten().sum()
        }
    }

    static fileSizeSort (DataflowChannel channel){
        from(
            channel.toList().get()
                .collect { it instanceof List ?
                it + it.collectNested { iit -> iit instanceof Path ? iit.toFile().length() / 1e9 : 0 }.flatten().sum() :
                it instanceof Path ? [it, it.toFile().length() / 1e9] : [it, 0] }
            .toSorted { a, b -> b.last() <=> a.last() }
                .collect { it.dropRight(1) } )
    }

    static toJson(DataflowQueue channel, Path path) {
        toJson(channel, path.toFile())
    }

    static toJson(DataflowQueue channel, String path) {
        toJson(channel, (new File(path)))
    }

    static toJson(DataflowQueue channel, File json) {
        def parent = json.toPath().toAbsolutePath().parent
        if (! parent.exists()) { parent.mkdirs() }
        def list = channel.toList().get().collectNested { it ->
            it instanceof java.nio.file.Path ? it.toRealPath().toAbsolutePath().toUri().toString() : it
        }
        json.write(JsonOutput.toJson(list) + '\n', 'utf-8')
        return from(json.toPath()).first()
    }

    static toTsv(DataflowQueue channel, String filename) {
        toDelim(channel, (new File(filename)), '\t', [])
    }

    static toCsv(DataflowQueue channel, String filename) {
        toDelim(channel, (new File(filename)), ',', [])
    }

    static toTsv(DataflowQueue channel, String filename, List colNames) {
        toDelim(channel, (new File(filename)), '\t', colNames)
    }

    static toCsv(DataflowQueue channel, String filename, List colNames) {
        toDelim(channel, (new File(filename)), ',', colNames)
    }

    static toDelim(DataflowQueue channel, File file, String delim, List colNames, archive = true) {
        def parent = file.toPath().toAbsolutePath().toFile().parentFile
        if (! parent.exists()) { parent.mkdirs() }
        if (archive) {
            file = archiveFile(file)
        }
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

    static subscribeToTsv(DataflowChannel channel, file) {
        subscribeToDelim(channel, file, '\t' )
    }

    static subscribeToCsv(DataflowChannel channel, file) {
        subscribeToDelim(channel, file, ',')
    }

    static subscribeToDelim(DataflowChannel channel, String file, String delim){
        subscribeToDelim(channel, (new File(file)), delim )
    }

    static subscribeToDelim(DataflowChannel channel, Path path, String delim){
        subscribeToDelim(channel, path.toFile(), delim )
    }

    static subscribeToDelim(DataflowChannel channel, File file, String delim='\t'){
        def parent = file.toPath().toAbsolutePath().toFile().parentFile
        if (! parent.exists()) { parent.mkdirs() }
        file.write('', 'utf-8')
        channel.subscribe {
            file.append( it.collect {
                it instanceof Path ? it.toRealPath().toAbsolutePath().toString() : it }
            .join(delim) + '\n', 'utf-8')
        }
    }

    static sortTuplesBy (DataflowChannel channel, Integer by, rev = false){
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