package tidynf

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import java.nio.file.Path
import org.codehaus.groovy.runtime.NullObject

import tidynf.extension.TidyDelegatingMetaClass

import static tidynf.io.DelimHandler.readDelim
import static tidynf.exception.TidyError.tidyError


class TidyMethods {

    static tidynf() {

        def dataflowQueueMetaClass = new TidyDelegatingMetaClass(DataflowQueue.metaClass, TidyOps)
        dataflowQueueMetaClass.initialize()
        DataflowQueue.metaClass = dataflowQueueMetaClass

        def dataflowVariableMetaClass = new TidyDelegatingMetaClass(DataflowVariable.metaClass, TidyOps)
        dataflowVariableMetaClass.initialize()
        DataflowVariable.metaClass = dataflowVariableMetaClass
    }

    static List as_file(List strings) {
        strings.collect { as_file(it.toString()) }
    }

    static Path as_file(String string){
        new File(string).toPath().toAbsolutePath()
    }

    static NullObject file_ext(NullObject nullObject, String ext){
        null
    }

    static List file_ext(List paths, String ext){
        paths.collect { file_ext(it as Path, ext) }
    }

    static Path file_ext(Path path, String ext){
        new File(path.toString() + ext).toPath().toAbsolutePath()
    }

    static NullObject file_replace(NullObject nullObject, String pattern, String replacement){
        null
    }

    static List file_replace(List paths,  String pattern, String replacement){
        paths.collect { file_replace(it as Path, pattern, replacement)}
    }

    static Path file_replace(Path path,  String pattern, String replacement){
        new File(path.toString().replaceAll(pattern, replacement)).toPath().toAbsolutePath()
    }

    static Float file_size(NullObject nullObject, String units = 'GB'){
        0 as float
    }

    static Float file_size(List paths, String units = 'GB') {
        paths.collect { file_size(it as Path, units) }.sum() as Float
    }

    static Float file_size(Path path, String units = 'GB'){
        def unit_defs = [B:1, KB:1e3, MB:1e6, GB:1e9]
        def length = new File(path.toString()).length()
        if (unit_defs.containsKey(units)){
            return length / unit_defs[units]
        } else {
            return length as Float
        }
    }

    static ArrayList read_csv(Object file, List col_names = null) {
        read_delim(file, ',', col_names)
    }

    static ArrayList read_tsv(Object file, List col_names=null) {
        read_delim(file, '\t', col_names)
    }

    static ArrayList read_delim(Object file, String delim, List col_names) {

        if (file instanceof String) {
            readDelim(file, delim, col_names)
        } else if (file instanceof Path) {
            readDelim(file, delim, col_names)
        } else if (file instanceof String) {
            readDelim(file, delim, col_names)
        } else {
            tidyError("argument file must be one of String, Path or File", "read_delim")
        }
    }

    static void write_tsv(List data, String filename) {
        write_delim(data, filename, '\t')
    }


    static void write_csv(List data, String filename) {
        write_delim(data, filename, ',')
    }


    static void write_delim(List data, String filename, String delim) {
        data
            .collect { it instanceof List ? it : it instanceof Map ? it.values() as List : [it] }
            .collect { it.collect { it.toString() } }
            .collect { it.join(delim) }
            .join('\n')
            .with { (new File(filename)).write(it + '\n', 'utf-8') }
    }
}