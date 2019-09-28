package tidynf

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import java.nio.file.Path

import tidynf.extension.TidyDelegatingMetaClass

import static tidynf.io.DelimHandler.readDelim
import static tidynf.exception.TidyError.tidyError


class TidyMethods {

    private static LinkedHashMap size_units = [B:1, KB:1e3, MB:1e6, GB:1e9]

    static tidynf() {

        def dataflowQueueMetaClass = new TidyDelegatingMetaClass(DataflowQueue.metaClass, TidyOps)
        dataflowQueueMetaClass.initialize()
        DataflowQueue.metaClass = dataflowQueueMetaClass

        def dataflowVariableMetaClass = new TidyDelegatingMetaClass(DataflowVariable.metaClass, TidyOps)
        dataflowVariableMetaClass.initialize()
        DataflowVariable.metaClass = dataflowVariableMetaClass
    }

    static float file_size(Path path, String units = 'GB'){

        units = units?.toUpperCase()
        if (! size_units.containsKey(units)) {
            tidyError("Units must be one of ${size_units.keySet().toString()}", 'file_size')
        }
        path?.toFile()?.length()?.div(size_units[units] as BigDecimal)?.with { it as float }
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