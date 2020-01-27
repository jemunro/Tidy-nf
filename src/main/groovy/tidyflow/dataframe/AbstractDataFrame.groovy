package tidyflow.dataframe

interface AbstractDataFrame {

    AbstractDataFrame anti_join(AbstractDataFrame right, String... by)

    AbstractDataFrame anti_join(AbstractDataFrame right, Set by)

    AbstractDataFrame arrange(Map par, String... by)

    AbstractDataFrame arrange(Map par, Set by)

    AbstractDataFrame arrange_all(Map par)

    AbstractDataFrame full_join(AbstractDataFrame right, String... by)

    AbstractDataFrame full_join(AbstractDataFrame right, Set by)

    AbstractDataFrame inner_join(AbstractDataFrame right, String... by)

    AbstractDataFrame inner_join(AbstractDataFrame right, Set by)

    AbstractDataFrame left_join(AbstractDataFrame right, String... by)

    AbstractDataFrame left_join(AbstractDataFrame right, Set by)

    AbstractDataFrame mutate(Closure closure)

    AbstractDataFrame mutate_with(Map with, Closure closure)

    AbstractDataFrame group_by(Map par, String... by)

    AbstractDataFrame group_by(Map par, Set by)

    Object pull(String var)

    AbstractDataFrame rename(Map nameMap)

    AbstractDataFrame right_join(AbstractDataFrame right, String... by)

    AbstractDataFrame right_join(AbstractDataFrame right, Set by)

    AbstractDataFrame select(String... vars)

    AbstractDataFrame select(Set vars)

    AbstractDataFrame semi_join(AbstractDataFrame right, String... by)

    AbstractDataFrame semi_join(AbstractDataFrame right, Set by)

    AbstractDataFrame unnest(String... at)

    AbstractDataFrame unnest(Set at)

    AbstractDataFrame unnest_all()

}