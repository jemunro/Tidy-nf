package tidynf

import java.nio.file.Path

class TidyHelpers {

    static List as_file(List strings) {
        strings.collect { as_file(it.toString()) }
    }

    static Path as_file(String string){
        new File(string).toPath().toAbsolutePath()
    }

    static Object file_ext(Object object, String ext){
         if (object instanceof Path){
             new File(object.toString() + ext).toPath().toAbsolutePath()
         } else if (object instanceof List) {
             object.collect { new File(it.toString() + ext).toPath().toAbsolutePath()}
         } else {
             throw new IllegalArgumentException()
         }
    }

    static Object file_replace(Object object, String pattern, String replacement) {
        if (object instanceof Path){
            new File(object.toString().replaceAll(pattern, replacement)).toPath().toAbsolutePath()
        } else if (object instanceof List) {
            object.collect { new File(it.toString().replaceAll(pattern, replacement)).toPath().toAbsolutePath() }
        } else {
            throw new IllegalArgumentException()
        }
    }

    static Float file_size(Object object, String units = 'GB') {
        def unit_defs = [B:1, KB:1e3, MB:1e6, GB:1e9]
        if (object instanceof Path){
            def length = new File(object.toString()).length()
            if (unit_defs.containsKey(units)){
                return length / unit_defs[units]
            } else {
                return length as Float
            }
        } else if (object instanceof List) {
            object.collect { it ->
                def length = new File(it.toString()).length()

                if (unit_defs.containsKey(units)){
                    return length / unit_defs[units]
                } else {
                    return length as Float
                }
            }.sum() as Float
        } else {
            throw new IllegalArgumentException()
        }
    }
}