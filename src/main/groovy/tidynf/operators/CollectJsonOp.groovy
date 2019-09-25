package tidynf.operators

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import java.nio.file.Path
import groovy.json.JsonGenerator

import static groovy.json.JsonOutput.prettyPrint
import static tidynf.TidyChecks.checkIsType
import static tidynf.TidyChecks.checkKeysMatch

class CollectJsonOp {

    private DataflowQueue source
    private boolean sort
    private File file

    private static final String method_name = 'collect_json'
    private static final generator = new JsonGenerator.Options()
        .addConverter(Path) { Path path, String key ->
            path.toAbsolutePath().toString()
        }
        .build()


    CollectJsonOp(DataflowQueue source, File file, Boolean sort) {

        this.source = source
        this.sort = sort
        this.file = file

    }

    DataflowVariable apply() {

        def parent = file.toPath().toAbsolutePath().toFile().parentFile
        if (! parent.exists()) { parent.mkdirs() }

        source.with { sort ? it.toSortedList() : it.toList() }.map {

            def list = it as ArrayList
            runChecks(list)
            file.write(prettyPrint(generator.toJson(list)) + '\n', 'utf-8')
            file.toPath()
        }
    }


    static void runChecks(List list) {

        list.collect { checkIsType(it, LinkedHashMap, method_name) }

        if (list.size() > 0) {
            def keySet = (list[0] as LinkedHashMap).keySet() as LinkedHashSet
            list.collect { checkKeysMatch(keySet, (it as LinkedHashMap).keySet() as LinkedHashSet, method_name ) }
        }
    }
}

