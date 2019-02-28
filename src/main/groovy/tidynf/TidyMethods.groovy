package tidynf

import channelextra.ChannelExtra
import channelextra.ChannelExtraOperators
import org.codehaus.groovy.runtime.NullObject
import java.nio.file.Path


class TidyMethods {

    static enableTidy() {
        ChannelExtra.enable(ChannelExtraOperators, TidyOperators)
    }

    static List as_file(List strings) {
        strings.collect { as_file(it.toString()) }
    }

    static Path as_file(String string){
        new File(string).toPath().toAbsolutePath()
    }

    static NullObject file_ext(NullObject nullObject, String ext){
        null
    }

    static List file_ext(List paths, String ext){
        paths.collect { file_ext(it as Path, ext) }
    }

    static Path file_ext(Path path, String ext){
        new File(path.toString() + ext).toPath().toAbsolutePath()
    }

    static NullObject file_replace(NullObject nullObject, String pattern, String replacement){
        null
    }

    static List file_replace(List paths,  String pattern, String replacement){
        paths.collect { file_replace(it as Path, pattern, replacement)}
    }

    static Path file_replace(Path path,  String pattern, String replacement){
        new File(path.toString().replaceAll(pattern, replacement)).toPath().toAbsolutePath()
    }

    static Float file_size(NullObject nullObject, String units = 'GB'){
        0 as float
    }

    static Float file_size(List paths, String units = 'GB') {
        paths.collect { file_size(it as Path, units) }.sum() as Float
    }

    static Float file_size(Path path, String units = 'GB'){
        def unit_defs = [B:1, KB:1e3, MB:1e6, GB:1e9]
        def length = new File(path.toString()).length()
        if (unit_defs.containsKey(units)){
            return length / unit_defs[units]
        } else {
            return length as Float
        }
    }
}