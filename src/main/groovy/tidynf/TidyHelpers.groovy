package tidynf

import java.nio.file.Path

class TidyHelpers {

    static List as_file(ArrayList strings) {
        strings.collect { as_file(it.toString()) }
    }

    static Path as_file(String string){
        new File(string).toPath().toAbsolutePath()
    }

    static List file_ext(List paths, String ext) {
        paths.collect { file_ext(it as Path, ext) }
    }

    static Path file_ext(Path path, String ext) {
        new File(path.toString() + ext).toPath().toAbsolutePath()
    }

    static List file_replace(List paths, String pattern, String replacement) {
        paths.collect { file_replace(it as Path, pattern, replacement) }
    }

    static Path file_replace(Path path, String pattern, String replacement) {
        new File(path.toString().replaceAll(pattern, replacement)).toPath().toAbsolutePath()
    }

    static Float file_size(List files, String units = 'GB'){
        files.collect { file_size(it as Path, units) }.sum() as Float
    }

    static Float file_size(Path path, String units = 'GB'){
        def length = new File(path.toString()).length()
        def unit_defs = [B:1, KB:1e3, MB:1e6, GB:1e9]
        if (unit_defs.containsKey(units)){
            return length / unit_defs[units]
        } else {
            return length as Float
        }
    }
}