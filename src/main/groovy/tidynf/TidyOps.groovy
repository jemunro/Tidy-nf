package tidynf

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.DataflowQueue

import tidynf.operators.ArrangeOp
import tidynf.operators.GroupByOp
import tidynf.operators.JoinOp
import tidynf.operators.MutateOp
import tidynf.operators.PullOp
import tidynf.operators.RenameOp
import tidynf.operators.SelectOp
import tidynf.operators.SetNamesOp
import tidynf.operators.ToColumnsOp
import tidynf.operators.ToRowsOp
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
        new ArrangeOp('arrange', params, channel, by).apply()
    }



    static DataflowQueue group_by(DataflowQueue queue, String... by) {
        group_by([:], queue, by as List)
    }

    static DataflowQueue group_by(Map params, DataflowQueue queue, String... by) {
        group_by(params, queue, by as List)
    }

    static DataflowQueue group_by(Map params, DataflowQueue queue, List by) {
        new GroupByOp('group_by', params, queue, by).apply()
    }



    static DataflowChannel mutate(DataflowChannel channel, Closure closure){
        new MutateOp('mutate', channel, closure).apply()
    }



    static DataflowChannel pull(DataflowChannel channel, String name){
        new PullOp('pull', channel, name).apply()
    }



    static DataflowChannel rename(DataflowChannel channel, String new_name, String old_name){
        new RenameOp('rename', channel, new_name, old_name).apply()
    }



    static DataflowChannel select(DataflowChannel channel, String... names){
        select(channel, names as List)
    }

    static DataflowChannel select(DataflowChannel channel, List names){
        new SelectOp('select', channel, names).apply()
    }



    static DataflowChannel set_names(DataflowChannel channel, String... names){
        set_names(channel, names as List)
    }

    static DataflowChannel set_names(DataflowChannel channel, List names){
        new SetNamesOp('set_names', channel, names).apply()
    }



    static DataflowVariable to_columns(DataflowQueue queue){
        new ToColumnsOp('to_columns', queue).apply()
    }



    static DataflowVariable to_rows(DataflowQueue queue){
        new ToRowsOp('to_rows', queue).apply()
    }



    static DataflowChannel unname(DataflowChannel channel){
        new UnnameOp('unname', channel).apply()
    }



    static DataflowChannel unnest(DataflowChannel channel, String... at) {
        unnest(channel, at as List).apply()
    }

    static DataflowChannel unnest(DataflowChannel channel, List at) {
        new UnnestOp('unnest', channel, at).apply()
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
}
