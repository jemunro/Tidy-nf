package tidyflow.dataframe

import groovyx.gpars.dataflow.DataflowChannel

class DataflowDataFrame implements AbstractDataFrame {

    private static Integer instanceCount = 0
    private DataflowChannel source
    private LinkedHashSet colNames
    private Integer instanceID

    DataflowDataFrame(DataflowChannel source){

        this.source = source
        this.instanceID = nextInstance()
    }

    static synchronized Integer nextInstance(){
        ++instanceCount
    }

    @Override
    DataflowDataFrame anti_join(AbstractDataFrame right, String... by) {
        return null
    }

    @Override
    DataflowDataFrame anti_join(AbstractDataFrame right, Set by) {
        return null
    }

    @Override
    DataflowDataFrame arrange(Map par, String... by) {
        return null
    }

    @Override
    DataflowDataFrame arrange(Map par, Set by) {
        return null
    }

    @Override
    DataflowDataFrame count_by(String... by) {
        return null
    }

    @Override
    DataflowDataFrame count_by(Set by) {
        return null
    }

    @Override
    DataflowDataFrame count_by_all() {
        return null
    }

    @Override
    DataflowDataFrame inner_join(AbstractDataFrame right, String... by) {
        return null
    }

    @Override
    DataflowDataFrame inner_join(AbstractDataFrame right, Set by) {
        return null
    }

    @Override
    DataflowDataFrame left_join(AbstractDataFrame right, String... by) {
        return null
    }

    @Override
    DataflowDataFrame left_join(AbstractDataFrame right, Set by) {
        return null
    }

    @Override
    DataflowDataFrame right_join(AbstractDataFrame right, String... by) {
        return null
    }

    @Override
    DataflowDataFrame right_join(AbstractDataFrame right, Set by) {
        return null
    }

    @Override
    DataflowDataFrame full_join(AbstractDataFrame right, String... by) {
        return null
    }

    @Override
    DataflowDataFrame full_join(AbstractDataFrame right, Set by) {
        return null
    }

    @Override
    DataflowDataFrame group_by(String... by) {
        return null
    }

    @Override
    DataflowDataFrame group_by(Set by) {
        return null
    }

    @Override
    DataflowDataFrame group_by_all() {
        return null
    }

    @Override
    DataflowDataFrame mutate(Closure closure) {
        return null
    }

    @Override
    DataflowDataFrame mutate_with(Map with, Closure closure) {
        return null
    }

    @Override
    DataflowDataFrame nest_by(String... by) {
        return null
    }

    @Override
    DataflowDataFrame nest_by(Set by) {
        return null
    }

    @Override
    DataflowDataFrame nest_by_all() {
        return null
    }

    @Override
    DataflowDataFrame rename(Map nameMap) {
        return null
    }

    @Override
    DataflowDataFrame select(String... vars) {
        return null
    }

    @Override
    DataflowDataFrame select(Set vars) {
        return null
    }

    @Override
    DataflowDataFrame semi_join(AbstractDataFrame right, String... by) {
        return null
    }

    @Override
    DataflowDataFrame semi_join(AbstractDataFrame right, Set by) {
        return null
    }

    @Override
    DataflowDataFrame summarize_by(String... by) {
        return null
    }

    @Override
    DataflowDataFrame summarize_by(Set by) {
        return null
    }

    @Override
    DataflowDataFrame unnest(String... at) {
        return null
    }

    @Override
    DataflowDataFrame unnest(Set at) {
        return null
    }

    @Override
    DataflowDataFrame unnest() {
        return null
    }

    @Override
    DataflowChannel pull(String var) {
        return null
    }
}
