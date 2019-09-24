package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import java.nio.file.Path

import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch
import static tidynf.exception.TidyError.tidyError

class CollectDelimOp {


    private String method_name
    private DataflowQueue source
    private boolean sort
    private String delim
    private File file
    private Boolean col_names
    private final static LinkedHashSet validMethods =  ["collect_delim", "collect_tsv", "collect_csv"]

    CollectDelimOp(DataflowQueue source, File file, String delim, Boolean col_names, Boolean sort, String method_name) {

        this.source = source
        this.sort = sort
        this.method_name = method_name
        this.delim = delim
        this.file = file
        this.col_names = col_names

        if (! validMethods.contains(method_name)) {
            tidyError("unknown collect_delim method: $method_name", "collect_delim")
        }
    }

    DataflowVariable apply() {

        def parent = file.toPath().toAbsolutePath().toFile().parentFile
        if (! parent.exists()) { parent.mkdirs() }

        source.with { sort ? it.toSortedList() : it.toList() }.map {

            def list = it as ArrayList
            runChecks(list)

            if (col_names) {
                file.write((list[0] as LinkedHashMap).keySet().join(delim) + '\n', 'utf-8')
            } else {
                file.write('', 'utf-8')
            }

            file.append(stringify(list) + '\n','utf-8')
            file.toPath()
        }
    }

    String stringify(List list) {

        list.collect { (it instanceof LinkedHashMap ? (it.values() as ArrayList) : (it as ArrayList)) }
            .collect { it.collect { it instanceof Path ? it.toAbsolutePath().toString() : it.toString() } }
            .collect { it.join(delim) }
            .join('\n')
    }

    void runChecks(List list) {

        list.collect { checkIsType(it, LinkedHashMap, method_name) }

        if (list.size() > 0) {
            def keySet = (list[0] as LinkedHashMap).keySet() as LinkedHashSet
            list.collect { checkKeysMatch(keySet, (it as LinkedHashMap).keySet() as LinkedHashSet, method_name ) }
        }
    }
}

