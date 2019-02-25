package tidynf

import groovyx.gpars.dataflow.DataflowChannel

import static nextflow.Nextflow.groupKey
import static tidynf.TidyValidator.*
import static tidynf.TidyDataFlow.splitKeysAndData

class TidyOperators {

    static DataflowChannel mapTidy(DataflowChannel channel){
        mapTidy(channel, { it } )
    }

    static DataflowChannel mapTidy(DataflowChannel channel, Closure closure){
        splitKeysAndData(channel).map {
            it.data = requireAsLinkedHashMap('mapTidy', it.data)
            checkKeysMatch('mapTidy', it.data.keySet() as List, it.keys as List)
            closure(it.data)
        }
    }

    static DataflowChannel select(DataflowChannel channel, String... names){
        select(channel, names as List)
    }

    static DataflowChannel select(DataflowChannel channel, List names){
        channel.mapTidy {
            checkKeysAreSubset('select', names, it.keySet() as List)
            names.collectEntries { k -> [(k): it[k]] }
        }
    }

    static DataflowChannel set_names(DataflowChannel channel, String... names){
        set_names(channel, names as List)
    }

    static DataflowChannel set_names(DataflowChannel channel, List names){
        checkUnique('set_names', names)
        channel.map {
            it = requireAsList('set_names', it)
            checkSize('set_names', names.size(), it.size())
            [names, it].transpose().collectEntries { k, v -> [(k): v] }
        }
    }

    static DataflowChannel unname(DataflowChannel channel){
        channel.mapTidy {
            it.collect { it.value }
        }
    }

    static DataflowChannel rename(DataflowChannel channel, String new_name, String old_name){
        channel.mapTidy {
            checkContains('rename', old_name, it.keySet() as List)
            checkContainsNot('rename', new_name, it.keySet() as List)
            if (it.containsKey(new_name)){
                throw new IllegalArgumentException("tidynf: new_name '${new_name}' already in Map")
            }
            it.collectEntries { k, v -> [(old_name == k ? new_name: k): v] }
        }
    }

    static DataflowChannel unnest(DataflowChannel channel){
        channel.mapTidy {
            def at = it.findAll { k, v -> v instanceof List }.collect { it.key }
            if (at.size() > 0){
                def n = it[at[0]].size()
                if (!(at.every {key -> it[key].size() == n })) {
                    tidyError('unnest', "All targets must be atomic or equally sized for unnest")
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
    }

    static DataflowChannel mutate(DataflowChannel channel, Closure closure){
        def parent_data = closure.binding.getVariables() as LinkedHashMap
        def dehydrated = closure.dehydrate()
        channel.mapTidy {
            def binding = new Binding()
            def data = parent_data + it
            def rehydrated = dehydrated.rehydrate(data, binding, binding)
            rehydrated.call()
            def res = binding.getVariables() as LinkedHashMap
            it + res
        }
    }

    static DataflowChannel group_by(DataflowChannel channel, String... by) {
        group_by(channel, by as List)
    }

    static DataflowChannel group_by(DataflowChannel channel, List by) {
        splitKeysAndData(channel).map {
            it.data = requireAsLinkedHashMap('group_by', it.data)
            checkKeysMatch('group_by', it.data.keySet() as List, it.keys as List)
            checkKeysAreSubset('group_by', by, it.keys)
            [ by.collect { k -> it.data[k] }, it.data ]
        }.groupTuple().map {
            it[1][0].keySet().collectEntries { k ->
                [(k): (by.contains(k) ? groupKey(it[1][0][k], it[1].size()) : it[1].collect { it[k] })]
            }
        }
    }



    static DataflowChannel arrange_by(DataflowChannel channel, String by){
        channel.map { it ->
            Helpers.requireLinkedHashMap(it)
            Helpers.requireKeys(it as LinkedHashMap, [by])
            def set = ([by]  +
                (it.findAll { k, v -> it[k] instanceof List ? it[k].size() == it[by].size() : false }.keySet() as List))
                .unique()
            def sorted = set
                .collect { k -> it[k] }
                .transpose()
                .sort { a, b -> a[0] <=> b[0] }
                .transpose()
                .withIndex()
                .collectEntries { item, i -> [(set[i]) : item]}
            it.collectEntries { k, v -> [(k): sorted.containsKey(k) ? sorted[k] : it[k]] }
        }
    }

    static DataflowChannel bind_rows(DataflowChannel channel){
        channel.toList().map { it ->
            if (!(it instanceof List)) {
                throw new IllegalArgumentException("tidynf: Expected List, got ${it.getClass()}")
            }
            if (!(it.every { it instanceof LinkedHashMap})) {
                throw new IllegalArgumentException("tidynf: Expected LinkedHashMap, got " +
                    "${it.find{ !(it instanceof LinkedHashMap) }.getClass() }")
            }
            def keyset = it[0].keySet()
            if (it.any { it.keySet() != keyset}) {
                throw new IllegalArgumentException("tidynf: Keyset mismatch, expected - $keyset," +
                    " got - ${ it.find {it.keySet() != keyset} }")
            }
            (keyset as List).collectEntries { k ->[(k): it.collect { it[k] }] }
        }
    }

    static DataflowChannel left_join(DataflowChannel left, DataflowChannel right, String... by) {
        def empty_right = null
        pre_join(left, right, by)
            .filter {(!(it.data.left.is(null)))}
            .map {
            if (!empty_right) {
                empty_right = it.right_keyset
                    .collectEntries { k -> [(k): null] }
                    .findAll { k, v -> !(by.contains(k)) }
            }
            it.data.left + (it.data.right.is(null) ? empty_right : it.data.right)
        }
    }

    static DataflowChannel right_join(DataflowChannel left, DataflowChannel right, String... by) {
        def empty_left = null
        pre_join(left, right, by)
            .filter {(!(it.data.right.is(null)))}
            .map {
            if (!empty_left) {
                empty_left = it.left_keyset
                    .collectEntries { k -> [(k): null] }
            }
            (it.data.left.is(null) ? empty_left : it.data.left) + it.data.right
        }
    }

    static DataflowChannel full_join(DataflowChannel left, DataflowChannel right, String... by) {
        def empty_left = null
        def empty_right = null
        pre_join(left, right, by)
            .map {
            if (!empty_left) {
                empty_left = it.left_keyset
                    .collectEntries { k -> [(k): null] }
            }
            if (!empty_right) {
                empty_right = it.right_keyset
                    .collectEntries { k -> [(k): null] }
                    .findAll { k, v -> !(by.contains(k)) }
            }
            (it.data.left.is(null) ? empty_left : it.data.left) + (it.data.right.is(null) ? empty_right : it.data.right)
        }
    }

    static DataflowChannel inner_join(DataflowChannel left, DataflowChannel right, String... by) {
        pre_join(left, right, by)
            .filter {(!(it.data.left.is(null) || it.data.right.is(null)))}
            .map {it.data.left + it.data.right }
    }

    private static DataflowChannel pre_join(DataflowChannel left, DataflowChannel right, String... by) {
        def by_ = by as List
        def left_keyset = null
        def right_keyset = null
        def left_aliases = null
        def right_aliases = null

        left.mapTidy { it ->
            joinable(it, by_)
            if (left_keyset) {
                checkKeysMatch(it, left_keyset)
            } else {
                left_keyset = it.keySet() as List
            }
            [by.collect { k -> it[k] }, it]
        }.join(right.mapTidy { it ->
            joinable(it, by_)
            if (right_keyset) {
                checkKeysMatch(it, right_keyset)
            } else {
                right_keyset = it.keySet() as List
            }
            [by_.collect { k -> it[k] }, it]
        }, remainder: true
        ).map { it ->
            if (!(left_aliases && right_aliases)) {
                def overlaps = left_keyset.findAll { k -> right_keyset.contains(k) && !by_.contains(k) }
                left_aliases = left_keyset
                    .collectEntries { k -> [(k): overlaps.contains(k) ? k + '_left' : k] }
                right_aliases = right_keyset
                    .collectEntries { k -> [(k): overlaps.contains(k) ? k + '_right' : k] }
            }
            [left : it[1].is(null) ? null : it[1].collectEntries { k, v -> [(left_aliases[k]): v] },
             right: it[2].is(null) ? null : it[2].collectEntries { k, v -> [(right_aliases[k]): v] }]
        }.map { it ->
            [data: it, left_keyset: left_aliases.values(), right_keyset: right_aliases.values()]
        }
    }

    static class Helpers {

        static void joinable(Object object, List by){
            requireLinkedHashMap(object)
            requireKeys(object as LinkedHashMap, by)
        }

        static void requireLinkedHashMap (Object object){
            if (!(object instanceof LinkedHashMap)) {
                throw new IllegalArgumentException("tidynf: Expected LinkedHashMap, got ${object.getClass()}")
            }
        }
        
        static void requireKeys(LinkedHashMap map, List keys){
            if (!(keys.every { map.containsKey(it) })) {
                throw new IllegalArgumentException(
                    "tidynf: keys '${ keys.findAll { name -> !object.containsKey(name)}}' not in Map")
            }
        }

        static void checkKeysMatch(LinkedHashMap data, List keys) {
            if ((data.keySet() as List) != keys){
                    throw new IllegalArgumentException(
                        "tidynf: key set mismatch - expected '$keys', got '${data.keySet() as List}'")
            }
        }
    }
}
