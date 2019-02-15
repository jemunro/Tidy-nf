package tidynf

import java.nio.file.Path

class TidyHelpers {

    static List as_file(List strings) {
        strings.collect { as_file(it.toString()) }
    }

    static Path as_file(String string){
        new File(string).toPath().toAbsolutePath()
    }

    static Object file_ext(List object, String ext){
         if (object instanceof Path){
             file_ext_path(object, ext)
         } else {
             file_ext_list(object, ext)
         }
    }

    static List file_ext_list(List paths, String ext) {
        paths.collect { file_ext_path(it as Path, ext) }
    }

    static Path file_ext_path(Path path, String ext) {
        new File(path.toString() + ext).toPath().toAbsolutePath()
    }

    static Object file_replace(List object, String pattern, String replacement) {
        if (object instanceof Path){
            file_replace_path(object, pattern ,replacement)
        } else {
            file_replace_list(object, pattern ,replacement)
        }
    }

    static List file_replace_list(List paths, String pattern, String replacement) {
        paths.collect { file_replac_path(it as Path, pattern, replacement) }
    }

    static Path file_replace_path(Path path, String pattern, String replacement) {
        new File(path.toString().replaceAll(pattern, replacement)).toPath().toAbsolutePath()
    }

    static Float file_size(List object, String units = 'GB') {
        if (object instanceof Path){
            file_size_path(object, units)
        } else {
            file_size_list(object, units)
        }
    }

    static Float file_size_list(List files, String units = 'GB') {
        files.collect { file_size_path(it as Path, units) }.sum() as Float
    }


    static Float file_size_path(Path path, String units = 'GB'){
        def length = new File(path.toString()).length()
        def unit_defs = [B:1, KB:1e3, MB:1e6, GB:1e9]
        if (unit_defs.containsKey(units)){
            return length / unit_defs[units]
        } else {
            return length as Float
        }
    }
}