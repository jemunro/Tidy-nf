package channelextra

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.DataflowChannel

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import java.nio.file.Path
import static nextflow.Channel.from
import static nextflow.Channel.create

class ChannelExtra {

    static File archiveFile(String filename){
        archiveFile(new File(filename))
    }

    static File archiveFile(File file){
        if (file.exists()) {
            def match = file.path =~ /(.*\.)([0-9]+)$/
            def archive_path = match ?
                match[0][1] + ((match[0][2] as Integer) + 1) : file.path + '.1'
            def archive_file = new File(archive_path)
            archiveFile(archive_file)
            file.renameTo(archive_file)
        }
        return file
    }

    static enable(){
        def dataflowQueueMetaClass =  new ChannelExtraDelegatingMetaClass(DataflowQueue.metaClass)
        dataflowQueueMetaClass.initialize()
        DataflowQueue.metaClass = dataflowQueueMetaClass

        def dataflowVariableMetaClass =  new ChannelExtraDelegatingMetaClass(DataflowVariable.metaClass)
        dataflowVariableMetaClass.initialize()
        DataflowVariable.metaClass = dataflowVariableMetaClass
    }

    static enable(Class... operators){
        def dataflowQueueMetaClass =  new ChannelExtraDelegatingMetaClass(DataflowQueue.metaClass, operators)
        dataflowQueueMetaClass.initialize()
        DataflowQueue.metaClass = dataflowQueueMetaClass

        def dataflowVariableMetaClass =  new ChannelExtraDelegatingMetaClass(DataflowVariable.metaClass, operators)
        dataflowVariableMetaClass.initialize()
        DataflowVariable.metaClass = dataflowVariableMetaClass
    }

    static disable(){
        DataflowQueue.metaClass = null
        DataflowVariable.metaClass = null
    }

    static create(Integer i){
        if (i > 1) {
            (1..i).collect { create() }
        } else {
            create()
        }
    }
    /*
        Methods to create channels from delimited files
     */
    static fromCsv(file, header = false) {
        fromDelim(file, ',', header)
    }

    static fromTsv(file, header = false) {
        fromDelim(file, '\t', header)
    }

    static fromDelim(String file, String delim = ',', header = false){
        fromDelim((new File(file)), delim, header)
    }

    static fromDelim(Path path, String delim = '\t', header = false){
        fromDelim(path.toFile(), delim, header)
    }

    static fromDelim(File file, String delim = '\t', header = false) {
        def lines = file.getText('utf-8').split('\n') as List
        if (header) { lines.removeAt(0) }
        return from(lines.collect { it.split(delim) as List })
    }

    //Methods to create channels from json files

    static fromJson(String filename) {
        fromJson(new File(filename))
    }
    static fromJson(java.nio.file.Path path) {
        fromJson(path.toFile())
    }
    static fromJson(File json){
        def slurper = new JsonSlurper()
        def list = slurper.parse(json).collectNested { it ->
            it instanceof String ? it.startsWith('file://') ? new File(new URI(it).path).toPath() : it : it
        }
        return from(list)
    }

    /* From DataflowVariable containing path to json file
    *  TODO: add error checks */
    static fromJson(DataflowVariable dataflowVariable) {
        def slurper = new JsonSlurper()
        dataflowVariable.flatMap {
            it = slurper.parse(it.toFile())
            it = it instanceof List ? it : [it]
            it.collectNested { iit ->
                iit instanceof String ? iit.startsWith('file://') ? new File(new URI(iit).path).toPath() : iit : iit
            }
        }
    }
}
