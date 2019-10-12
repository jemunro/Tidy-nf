package tidyflow

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import tidyflow.dataframe.DataFrame
import tidyflow.dataframe.DataflowDataFrame
import tidyflow.exception.IllegalTypeException
import tidyflow.dataframe.RowListDataFrame
import tidyflow.dataframe.ColMapDataFrame

import java.nio.file.Path

//import tidyflow.extension.TidyDelegatingMetaClass

import static tidyflow.exception.Message.errMsg
import static tidyflow.io.DelimHandler.readDelim
import static tidyflow.exception.Message.tidyError
import static tidyflow.io.DelimHandler.writeDelim
import static tidyflow.helpers.Predicates.isMapOfList


class TidyMethods {

    private static LinkedHashMap size_units = [B:1, KB:1e3, MB:1e6, GB:1e9]

//    static tidyflow() {
//
//        def dataflowQueueMetaClass = new TidyDelegatingMetaClass(DataflowQueue.metaClass, TidyOps)
//        dataflowQueueMetaClass.initialize()
//        DataflowQueue.metaClass = dataflowQueueMetaClass
//
//        def dataflowVariableMetaClass = new TidyDelegatingMetaClass(DataflowVariable.metaClass, TidyOps)
//        dataflowVariableMetaClass.initialize()
//        DataflowVariable.metaClass = dataflowVariableMetaClass
//    }

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
            throw new IllegalTypeException(errMsg('read_delim',
                    'argument file must be one of String, Path or File'))
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
            throw new IllegalTypeException(errMsg('write_delim',
                    'argument file must be one of String, Path or File'))
        }
    }

    static DataFrame as_df(ArrayList data) {
        data as RowListDataFrame
    }

    static DataFrame as_df(LinkedHashMap data) {
        if (isMapOfList(data)){
            data as ColMapDataFrame
        } else {
            [data] as RowListDataFrame
        }
    }

    static as_dfc(DataflowChannel channel){
        channel as DataflowDataFrame
    }

//    static DataflowDataFrame data_frame(DataflowChannel channel) {
//        channel as DataflowDataFrame
//    }
}