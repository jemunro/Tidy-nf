package tidynf.io

import java.nio.file.Path
import groovy.json.JsonGenerator
import groovy.json.JsonSlurper

import static groovy.json.JsonOutput.prettyPrint
import static tidynf.exception.TidyError.tidyError

class JsonHandler {

    private static final generator = new JsonGenerator.Options()
            .addConverter(Path) { Path path, String key ->
                path.toAbsolutePath().toString()
            }
            .build()

    private static final slurper = new JsonSlurper()

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

    static Object fromJson(File file) {
        slurper.parse(file)
    }

    static Object fromJson(Path path) {
        slurper.parse(path.toFile())
    }

    static Object fromJson(String string) {
        slurper.parseText(string)
    }

}
