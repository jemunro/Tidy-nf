package tidyflow.dataframe

interface AbstractDataFrame {

    AbstractDataFrame arrange(Map par, String... by)

    AbstractDataFrame arrange(Map par, Set by)

    AbstractDataFrame arrange_all(Map par)

    AbstractDataFrame mutate(Closure closure)

    AbstractDataFrame mutate_with(Map with, Closure closure)

    AbstractDataFrame nest_by(Map par, String... by)

    AbstractDataFrame nest_by(Map par, Set by)

    Object pull(String var)

    AbstractDataFrame rename(Map nameMap)

    AbstractDataFrame select(String... vars)

    AbstractDataFrame select(Set vars)

    AbstractDataFrame unnest(String... at)

    AbstractDataFrame unnest(Set at)

    AbstractDataFrame unnest_all()

}