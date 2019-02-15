package tidynf

class TidyMappers {

    static Closure select(String... names) {
        select(names as List)
    }

    static Closure select(List names){
        if (!(names.every {it instanceof String })) {
            throw new IllegalArgumentException("tidynf: Expected List<String>, got ${names.collect{it.getClass()}}")
        }
        { it ->
            if (!(it instanceof LinkedHashMap)) {
                throw new IllegalArgumentException("tidynf: Expected LinkedHashMap, got ${it.getClass()}")
            }
            if (!(names.every { name -> it.containsKey(name) })){
                throw new IllegalArgumentException(
                    "tidynf: names '${names.findAll { name -> !it.containsKey(name)}}' not in Map")
            }
            names.collectEntries { k -> [(k): it[k]] }
        }
    }

    static Closure set_names(String... names) {
        set_names(names as List)
    }

    static Closure set_names(List names){
        if (!(names.every { it instanceof String })) {
            throw new IllegalArgumentException("tidynf: Expected List<String>, got ${names.collect{it.getClass()}}")
        }
        { it ->
            if (it instanceof LinkedHashMap) {
                it = it.collect { it.value }
            }
            if (!(it instanceof List)) {
                throw new IllegalArgumentException("tidynf: Expected List, got ${it.getClass()}")
            }
            if (!(it.size() == names.size())){
                throw new IllegalArgumentException("tidynf: size of names must be equal to size of target")
            }
            [names, it].transpose().collectEntries { k, v -> [(k): v] }
        }
    }

    static Closure rename(String new_name, String old_name){
        if (!(old_name instanceof String )) {
            throw new IllegalArgumentException("tidynf: Expected String, got ${old_name.getClass()}")
        }
        if (!(new_name instanceof String)) {
            throw new IllegalArgumentException("tidynf: Expected String, got ${new_name.getClass()}")
        }
        if (new_name == old_name){
            throw new IllegalArgumentException("tidynf: new_name and old_name are identical: '$new_name'")
        } else {
            { it ->
                if (!(it instanceof LinkedHashMap)) {
                    throw new IllegalArgumentException("tidynf: Expected LinkedHashMap, got ${it.getClass()}")
                }
                if (!(it.containsKey(old_name))){
                    throw new IllegalArgumentException("tidynf: old_name '${old_name}' not in Map")
                }
                if (it.containsKey(new_name)){
                    throw new IllegalArgumentException("tidynf: new_name '${new_name}' already in Map")
                }
                it.collectEntries { k, v -> [(old_name == k ? new_name: k): v] }
            }
        }
    }

    static Closure unname(){
        { it ->
            if (it instanceof LinkedHashMap) {
                it.collect { it.value }
            } else {
                it
            }
        }
    }

    static Closure unnest() {
        //TODO - add arguments to allow unnesting of subset of entries
        { it ->
            if (it instanceof LinkedHashMap) {
                def at = it.findAll { k, v -> v instanceof List }.collect { it.key }
                if (at.size() > 0){
                    def n = it[at[0]].size()
                    if (!(at.every {key -> it[key].size() == n })) {
                        throw new IllegalArgumentException("tidynf: All targets must be atomic or equally sized for unnest")
                    }
                    (0..<n).collect { i ->
                        it.collectEntries { k, v ->
                            [(k): at.contains(k) ? it[k][i] : it[k]]
                        }
                    }
                } else {
                    it
                }
            }

            else if (it instanceof List) {
                def at = it.withIndex().findAll { x, i -> x instanceof List }.collect { it[1] }
                if (at.size() > 0){
                    def m = it.size()
                    def n = it[at[0]].size()
                    if (!(it[at].every {item -> item.size() == n })) {
                        throw new IllegalArgumentException("tidynf: All targets must be atomic or equally sized for unnest")
                    }
                    (0..<n).collect { i ->
                        (0..<m).collect { j ->
                            at.contains(j) ? it[j][i] : it[j]
                        }
                    }
                } else {
                    it
                }

            } else {
                throw new IllegalArgumentException("tidynf: Expected List or LinkedHashMap, got ${it.getClass()}")
            }
        }
    }

    static Closure unnest2(keep_outer = true) {
        { it ->
            if (!(it instanceof LinkedHashMap)) {
                throw new IllegalArgumentException("tidynf: Expected LinkedHashMap, got ${it.getClass()}")
            }
            def at = it.findAll { k, v -> v instanceof List }.collect { it.key }
            def at2 = it.findAll { k, v -> v instanceof List ? v.every { it instanceof LinkedHashMap } : false }
                .collect { it.key }
            if (at.size() > 0){
                def n = it[at[0]].size()
                if (!(at.every {key -> it[key].size() == n })) {
                    throw new IllegalArgumentException("tidynf: All targets must be atomic or equally sized for unnest")
                }
                (0..<n).collect { i ->
                    it.collectEntries { k, v ->
                        [(k): at.contains(k) ? it[k][i] : it[k]]
                    }
                }.collect {
                    if (keep_outer) {
                        at2.forEach { k -> it = it[k] + it; it.remove(k) }
                    } else {
                        at2.forEach { k -> it = it + it[k]; it.remove(k) }
                    }
                    it
                }
            } else {
                it
            }
        }
    }

    static Closure mutate(Closure closure) {
        def parent_data = closure.binding.getVariables() as LinkedHashMap
        def dehydrated = closure.dehydrate()
        return { it ->
            if (!(it instanceof LinkedHashMap)) {
                throw new IllegalArgumentException("tidynf: Expected LinkedHashMap, got ${it.getClass()}")
            }
            def binding = new Binding()
            def data = parent_data + it
            def rehydrated = dehydrated.rehydrate(data, binding, binding)
            rehydrated.call()
            def res = binding.getVariables() as LinkedHashMap
            it + res
        }
    }
}

