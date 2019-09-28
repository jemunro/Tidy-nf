package tidynf.io

import java.nio.file.Path
import static tidynf.helpers.TidyChecks.checkAllAreType
import static tidynf.helpers.TidyChecks.checkNonEmpty

class DelimHandler {

    static List readDelim(Path path, String delim, List colNames = null) {
        readDelim(path.toFile(), delim, colNames)
    }

    static List readDelim(String filename, String delim, List colNames = null) {
        readDelim(new File(filename), delim, colNames)
    }

    static List readDelim(File file, String delim, List colNames = null) {

        ArrayList split = splitLines(file, delim)
        if (colNames) {
            checkAllAreType(colNames, String, 'read_delim')
        } else {
            colNames = split[0] as List
            split = split.drop(1)
        }

        split.collect { [colNames, it].transpose().collectEntries { k, v -> [(k): v] } }
    }

    static ArrayList splitLines(File file, String delim) {

        (file.getText().split('\n') as ArrayList).collect { it.split(delim) as ArrayList }
    }

    static void writeDelim(List data, Path path, String delim, Boolean colNames, Boolean append) {
        writeDelim(data, path.toFile(), delim, colNames, append)
    }

    static void writeDelim(List data, String filename, String delim, Boolean colNames, Boolean append) {
        writeDelim(data, new File(filename), delim, colNames, append)
    }

    static void writeDelim(List data, File file, String delim, Boolean colNames, Boolean append) {

        checkNonEmpty(data, 'writeDelim')
        File parent = file.toPath().toAbsolutePath().toFile().parentFile
        if (! parent.exists()) { parent.mkdirs() }

        String text = ''
        if (colNames && ! append) {
            if (data[0] instanceof LinkedHashMap) {
                text += (data[0] as LinkedHashMap).keySet().join(delim) + '\n'
            }
        }
        text += stringify(data, delim) + '\n'
        if (append) {
            file.append(text, 'utf-8')
        } else {
            file.write(text, 'utf-8')
        }
    }

    static String stringify(List data, String delim) {

        data.collect {
            (it instanceof LinkedHashMap ?
                    (it.values() as ArrayList) :
                    (it instanceof ArrayList ? it : [it] as ArrayList)) }
                .collect { it.collect { it instanceof Path ? it.toAbsolutePath().toString() : it.toString() } }
                .collect { it.join(delim) }
                .join('\n')
    }
}
