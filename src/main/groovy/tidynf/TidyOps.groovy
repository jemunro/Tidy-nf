package tidynf

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.DataflowQueue

import tidynf.operators.ArrangeOp
import tidynf.operators.CollectJsonOp
import tidynf.operators.GroupByOp
import tidynf.operators.JoinOp
import tidynf.operators.MutateOp
import tidynf.operators.PullOp
import tidynf.operators.RenameOp
import tidynf.operators.SelectOp
import tidynf.operators.SetNamesOp
import tidynf.operators.CollectColsOp
import tidynf.operators.CollectRowsOp
import tidynf.operators.CollectDelimOp
import tidynf.operators.SubscribeDelimOp
import tidynf.operators.UnnameOp
import tidynf.operators.UnnestOp


class TidyOps {

    static DataflowChannel arrange(DataflowChannel channel, String... by) {
        arrange([:], channel, by as List)
    }

    static DataflowChannel arrange(Map params, DataflowChannel channel, String... by) {
        arrange(params, channel, by as List)
    }

    static DataflowChannel arrange(Map params, DataflowChannel channel, List by) {
        new ArrangeOp(params, channel, by).apply()
    }

    static DataflowQueue group_by( DataflowQueue queue, String... by) {
        group_by(queue, by as List)
    }

    static DataflowQueue group_by(DataflowQueue queue, List by) {
        new GroupByOp(queue, by).apply()
    }


    static DataflowChannel mutate(DataflowChannel channel, Closure closure){
        mutate([:], channel, closure)
    }


    static DataflowChannel mutate(Map with, DataflowChannel channel, Closure closure){
        new MutateOp(channel, closure, with).apply()
    }


    static DataflowChannel pull(DataflowChannel channel, String name){
        new PullOp(channel, name).apply()
    }


    static DataflowChannel rename(DataflowChannel channel, String new_name, String old_name){
        new RenameOp(channel, new_name, old_name).apply()
    }


    static DataflowChannel select(DataflowChannel channel, String... names){
        select(channel, names as List)
    }

    static DataflowChannel select(DataflowChannel channel, List names){
        new SelectOp(channel, names).apply()
    }


    static DataflowChannel set_names(DataflowChannel channel, String... names){
        set_names(channel, names as List)
    }

    static DataflowChannel set_names(DataflowChannel channel, List names){
        new SetNamesOp(channel, names).apply()
    }


    static DataflowVariable collect_cols(DataflowQueue queue, sort = true) {
        new CollectColsOp(queue, sort).apply()
    }


    static DataflowVariable collect_rows(DataflowQueue queue, sort = true){
        new CollectRowsOp(queue, sort).apply()
    }


    static DataflowChannel unname(DataflowChannel channel){
        new UnnameOp(channel).apply()
    }

    static DataflowChannel unnest(DataflowChannel channel, String... at) {
        unnest(channel, at as List)
    }

    static DataflowChannel unnest(DataflowChannel channel, List at) {
        new UnnestOp(channel, at).apply()
    }

    static DataflowQueue left_join(DataflowQueue left, DataflowQueue right, String... by) {
        left_join(left, right, by as List)
    }

    static DataflowQueue left_join(DataflowQueue left, DataflowQueue right, List by) {
        new JoinOp('left_join', left, right, by).apply()
    }

    static DataflowQueue right_join(DataflowQueue left, DataflowQueue right, String... by) {
        right_join(left, right, by as List)
    }

    static DataflowQueue right_join(DataflowQueue left, DataflowQueue right, List by) {
        new JoinOp('right_join', left, right, by).apply()
    }

    static DataflowQueue full_join(DataflowQueue left, DataflowQueue right, String... by) {
        full_join(left, right, by as List)
    }

    static DataflowQueue full_join(DataflowQueue left, DataflowQueue right, List by) {
        new JoinOp('full_join', left, right, by).apply()
    }

    static DataflowQueue inner_join(DataflowQueue left, DataflowQueue right, String... by) {
        inner_join(left, right, by as List)
    }

    static DataflowQueue inner_join(DataflowQueue left, DataflowQueue right, List by) {
        new JoinOp('inner_join', left, right, by).apply()
    }

    static DataflowVariable collect_tsv(DataflowChannel source, String filename, col_names = true, sort = true) {
        collect_tsv(source, new File(filename), col_names, sort)
    }

    static DataflowVariable collect_tsv(DataflowChannel source, File file, col_names = true, sort = true) {
        new CollectDelimOp(source, file, '\t', col_names, sort, 'collect_tsv').apply()
    }

    static DataflowVariable collect_csv(DataflowChannel source, String filename, col_names = true, sort = true) {
        collect_csv(source, new File(filename), col_names, sort)
    }

    static DataflowVariable collect_csv(DataflowChannel source, File file, col_names = true, sort = true) {
        new CollectDelimOp(source, file, ',', col_names, sort, 'collect_csv').apply()
    }

    static DataflowQueue subscribe_csv(DataflowQueue source, String filename, Boolean col_names = true) {
        subscribe_csv(source, new File(filename), col_names)
    }

    static DataflowQueue subscribe_csv(DataflowQueue source, File file, Boolean col_names = true) {
        new SubscribeDelimOp(source, file, ',', col_names, 'subscribe_csv').apply()
    }

    static DataflowQueue subscribe_tsv(DataflowQueue source, String filename, Boolean col_names = true) {
        subscribe_tsv(source, new File(filename), col_names)
    }

    static DataflowQueue subscribe_tsv(DataflowQueue source, File file, Boolean col_names = true) {
        new SubscribeDelimOp(source, file, '\t', col_names, 'subscribe_tsv').apply()
    }

    static DataflowVariable collect_json(DataflowChannel source, String filename, sort = true) {
        collect_json(source, new File(filename), sort)
    }

    static DataflowVariable collect_json(DataflowChannel source, File file, sort = true) {
        new CollectJsonOp(source, file, sort).apply()
    }
}
