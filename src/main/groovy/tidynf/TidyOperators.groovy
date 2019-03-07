package tidynf

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.DataflowQueue

import static nextflow.Nextflow.groupKey
import static TidyChecker.*
import static tidynf.TidyDataFlow.*

class TidyOperators {

    static DataflowChannel map_tidy(DataflowChannel channel, String method){
        map_tidy(channel, method, { it } )
    }

    static DataflowChannel map_tidy(DataflowChannel channel, Closure closure){
        map_tidy(channel, 'map_tidy', closure)
    }

    static DataflowChannel map_tidy(DataflowChannel channel){
        map_tidy(channel, 'map_tidy', { it } )
    }

    static DataflowChannel map_tidy(DataflowChannel channel, String method, Closure closure){

        withKeys(channel).map {
            it.data = requireAsLinkedHashMap(method, it.data)
            checkKeysMatch(method, it.data.keySet() as List, it.keys as List)
            closure(it.data)
        }
    }

    static DataflowChannel pull(DataflowChannel channel, String name){

        def method = 'pull'
        channel.map_tidy(method) {
            checkKeysAreSubset(method, [name], it.keySet() as List)
            it[name]
        }
    }

    static DataflowChannel select(DataflowChannel channel, String... names){
        select(channel, names as List)
    }

    static DataflowChannel select(DataflowChannel channel, List names){

        def method = 'select'
        channel.map_tidy(method) {
            checkKeysAreSubset(method, names, it.keySet() as List)
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
            checkSetNames('set_names', it , names)
            [names, it].transpose().collectEntries { k, v -> [(k): v] }
        }
    }

    static DataflowChannel unname(DataflowChannel channel){

        def method = 'unname'
        channel.map_tidy(method)  {
            it.collect { it.value }
        }
    }

    static DataflowChannel rename(DataflowChannel channel, String new_name, String old_name){

        def method = 'rename'

        channel.map_tidy(method) {
            checkContains(method, old_name, it.keySet() as List)
            checkContainsNot(method, new_name, it.keySet() as List)
            if (it.containsKey(new_name)){
                throw new IllegalArgumentException("tidynf: new_name '${new_name}' already in Map")
            }
            it.collectEntries { k, v -> [(old_name == k ? new_name: k): v] }
        }
    }

    static DataflowChannel unnest(DataflowChannel channel, String... at) {
        unnest(channel, at as List)
    }

    static DataflowChannel unnest(DataflowChannel channel, List at){

        def method = 'unnest'
        channel.map_tidy(method) {
            if (! at){
                at = it.findAll { k, v -> v instanceof List }.collect { it.key }
            } else {
                checkKeysAreSubset(method, at, it.keySet() as List)
                at = at.findAll { k -> it[k] instanceof List }
            }
            if (! at) {
                [ it ]
            } else {
                checkEqualSizes(method, at.collect { k -> it[k] })
                def n = it[at[0]].size()
                (0..<n).collect { i ->
                    it.collectEntries { k, v ->
                        [(k): at.contains(k) ? it[k][i] : it[k]]
                    }
                }
            }
        }.flatMap { it }
    }

    static DataflowVariable to_columns(DataflowQueue queue){

        def method = 'to_columns'
        queue.map_tidy(method).toList()
            .map { (it[0].keySet() as List).collectEntries{ k -> [ (k): it.collect { it[k] } ] } }
    }

    static DataflowVariable to_rows(DataflowQueue queue){

        def method = 'to_rows'
        queue.map_tidy(method).toList()
    }

    static DataflowChannel mutate(DataflowChannel channel, Closure closure){

        def method = 'mutate'
        def parent_data = closure.binding.getVariables() as LinkedHashMap
        def dehydrated = closure.dehydrate()

        channel.map_tidy(method) {
            def binding = new Binding()
            def data = parent_data + it
            def rehydrated = dehydrated.rehydrate(data, binding, binding)
            rehydrated.call()
            def res = binding.getVariables() as LinkedHashMap
            it + res
        }
    }

    static DataflowChannel group_by(DataflowChannel channel, String... by) {
        group_by([:], channel, by as List)
    }

    static DataflowChannel group_by(Map params, DataflowChannel channel, String... by) {
        group_by(params, channel, by as List)
    }

    static DataflowChannel group_by(DataflowChannel channel, List by) {
        group_by([:], channel, by)
    }

    static DataflowChannel group_by(Map params, DataflowChannel channel, List by) {
        /*
            Todo: dynamic group_size using groupKey framework
         */

        def method = 'group_by'
        def required = []
        def types = [group_key: Boolean]
        checkRequiredParams(method, required, params)
        checkParamTypes(method, types, params)
        def group_key = params?.group_key ?: false

        withKeys(channel).map {
            it.data = requireAsLinkedHashMap(method, it.data)
            checkKeysMatch(method, it.data.keySet() as List, it.keys as List)
            checkKeysAreSubset(method, by, it.keys)
            [ ( by.size() > 1 ? by.collect { k -> it.data[k] } : it.data[by[0]] ), it.data ]
        }.groupTuple().map {
            it[1][0].keySet().collectEntries { k ->
                [(k): (by.contains(k) ?
                    (group_key ? groupKey(it[1][0][k], it[1].size()) : it[1][0][k] ) :
                    ( it[1].collect { it[k] } )
                )]
            }
        }
    }

    static DataflowChannel arrange(Map params, DataflowChannel channel, String... by) {
        arrange(params, channel, by as List)
    }

    static DataflowChannel arrange(DataflowChannel channel, String... by) {
        arrange([:], channel, by as List)
    }

    static DataflowChannel arrange(Map params, DataflowChannel channel, List by){

        def method = 'arrange'
        def types = [not:List, not_:String, at:List, at_:String, reverse: Boolean]
        def required = []
        checkRequiredParams(method, required, params)
        checkParamTypes(method, types, params)
        def reverse = params?.reverse ?: false
        def at = params?.at ?: []
        def not = params?.not ?: []

        channel.map_tidy(method) {
            checkKeysAreSubset(method, by, it.keySet() as List)
            def set = null
            if (at) {
                checkNoOverlap(method, at, by)
                checkKeysAreSubset(method, at, it.keySet() as List)
                set = by + at
                checkEqualSizes(method, set.collect { k -> it[k] } )
            } else {
                checkEqualSizes(method, by.collect { k -> it[k]} )
                set = it
                    .findAll { k, v -> it[k] instanceof List && ! by.contains(k) && ! not.contains(k) }
                    .findAll { item -> item.value.size() == it[by[0]].size() }
                    .with { it.keySet() as List }
                    .with { by + it }
            }
            def sorted = set
                .collect { k -> it[k] }
                .transpose()
                .collect { [it.take(by.size()), it.takeRight(it.size() - by.size())] }
                .sort { l1, l2 ->
                    [l1[0], l2[0]].transpose()
                        .find { e1, e2 -> e1 != e2 }
                        .with { it ? it[0] <=> it[1] : 0 } }
                .with { reverse ? it.reverse() : it }
                .collect { it[0] + it[1] }
                .transpose()
                .withIndex()
                .collectEntries { item, i -> [(set[i]) : item] }

            it.collectEntries { k, v -> [(k): sorted.containsKey(k) ? sorted[k] : it[k]] }
        }
    }


    static DataflowChannel left_join(DataflowChannel left, DataflowChannel right, String... by) {
        left_join(left, right, by as List)
    }

    static DataflowChannel left_join(DataflowChannel left, DataflowChannel right, List by) {
        preJoin(left, right, by)
            .filter { it.contains_left }
            .map { it.by + it.left_data + it.right_data  }
    }

    static DataflowChannel right_join(DataflowChannel left, DataflowChannel right, String... by) {
        right_join(left, right, by as List)
    }

    static DataflowChannel right_join(DataflowChannel left, DataflowChannel right, List by) {
        preJoin(left, right, by)
            .filter { it.contains_right }
            .map { it.by + it.left_data + it.right_data  }
    }

    static DataflowChannel full_join(DataflowChannel left, DataflowChannel right, String... by) {
        full_join(left, right, by as List)
    }

    static DataflowChannel full_join(DataflowChannel left, DataflowChannel right, List by) {
        preJoin(left, right, by)
            .map { it.by + it.left_data + it.right_data }
    }

    static DataflowChannel inner_join(DataflowChannel left, DataflowChannel right, String... by) {
        inner_join(left, right, by as List)
    }

    static DataflowChannel inner_join(DataflowChannel left, DataflowChannel right, List by) {
        preJoin(left, right, by)
            .filter {  it.contains_right && it.contains_left }
            .map { it.by + it.left_data + it.right_data }
    }
}
