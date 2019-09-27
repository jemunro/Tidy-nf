package tidynf.io

import groovy.json.JsonGenerator
import java.nio.file.Path
import static tidynf.exception.TidyError.tidyError

import static groovy.json.JsonOutput.prettyPrint

class JsonWriter {

    private static final generator = new JsonGenerator.Options()
            .addConverter(Path) { Path path, String key ->
                path.toAbsolutePath().toString()
            }
            .build()

    static writeJson(Object object, File file) {
        file.write(toJson(object) + '\n', 'utf-8')
    }

    static toJson(Object object) {
        try {
            prettyPrint(generator.toJson(object))
        } catch (StackOverflowError e) {
            tidyError("Failed to convert object ${object.toString()} to json", "JsonWriter")
        }
    }

}
