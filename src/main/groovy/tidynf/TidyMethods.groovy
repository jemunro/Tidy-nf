package tidynf

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import java.nio.file.Path

import tidynf.extension.TidyDelegatingMetaClass

import static tidynf.io.DelimHandler.readDelim
import static tidynf.exception.Message.tidyError
import static tidynf.io.DelimHandler.writeDelim


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
        } else if (file instanceof File) {
            readDelim(file, delim, col_names)
        } else {
            tidyError("argument file must be one of String, Path or File", "read_delim")
        }
    }

    static void write_tsv(List data, Object file, Boolean col_names = true) {
        write_delim(data, file, '\t', col_names)
    }

    static void write_csv(List data, Object file, Boolean col_names = true) {
        write_delim(data, file, ',', col_names)
    }

    static void write_delim(List data, Object file, String delim, Boolean col_names = true) {

        if (file instanceof String) {
            writeDelim(data, file, delim, col_names, false)
        } else if (file instanceof Path) {
            writeDelim(data, file, delim, col_names, false)
        } else if (file instanceof File) {
            writeDelim(data, file, delim, col_names, false)
        } else {
            tidyError("argument file must be one of String, Path or File", "write_delim")
        }
    }
}