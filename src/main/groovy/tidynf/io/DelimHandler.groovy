package tidynf.io

import java.nio.file.Path
import static tidynf.TidyChecks.checkAllAreType

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
}
