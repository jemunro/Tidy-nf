package tidynf

import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
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
import tidynf.operators.ToGroupSizeOp
import tidynf.operators.ToRowsOp
import tidynf.operators.UnnameOp
import tidynf.operators.UnnestOp


class TidyOps {

    static DataflowWriteChannel arrange(DataflowWriteChannel channel, String... by) {
        arrange([:], channel, by as List)
    }

    static DataflowWriteChannel arrange(Map params, DataflowWriteChannel channel, String... by) {
        arrange(params, channel, by as List)
    }

    static DataflowWriteChannel arrange(Map params, DataflowWriteChannel channel, List by) {
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


    static DataflowWriteChannel mutate(DataflowWriteChannel channel, Closure closure){
        mutate([:], channel, closure)
    }


    static DataflowWriteChannel mutate(Map with, DataflowWriteChannel channel, Closure closure){
        new MutateOp('mutate', channel, closure, with).apply()
    }


    static DataflowWriteChannel pull(DataflowWriteChannel channel, String name){
        new PullOp('pull', channel, name).apply()
    }



    static DataflowWriteChannel rename(DataflowWriteChannel channel, String new_name, String old_name){
        new RenameOp('rename', channel, new_name, old_name).apply()
    }



    static DataflowWriteChannel select(DataflowWriteChannel channel, String... names){
        select(channel, names as List)
    }

    static DataflowWriteChannel select(DataflowWriteChannel channel, List names){
        new SelectOp('select', channel, names).apply()
    }



    static DataflowWriteChannel set_names(DataflowWriteChannel channel, String... names){
        set_names(channel, names as List)
    }

    static DataflowWriteChannel set_names(DataflowWriteChannel channel, List names){
        new SetNamesOp('set_names', channel, names).apply()
    }



    static DataflowVariable to_columns(DataflowQueue queue){
        new ToColumnsOp('to_columns', queue).apply()
    }



    static DataflowVariable to_rows(DataflowQueue queue){
        new ToRowsOp('to_rows', queue).apply()
    }



    static DataflowVariable to_group_size(DataflowQueue queue, String... by) {
        to_group_size(queue, by as List)
    }

    static DataflowVariable to_group_size(DataflowQueue queue, List by) {
        new ToGroupSizeOp('to_group_size', queue, by).apply()
    }



    static DataflowWriteChannel unname(DataflowWriteChannel channel){
        new UnnameOp('unname', channel).apply()
    }



    static DataflowWriteChannel unnest(DataflowWriteChannel channel, String... at) {
        unnest(channel, at as List)
    }

    static DataflowWriteChannel unnest(DataflowWriteChannel channel, List at) {
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
