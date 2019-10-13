package tidyflow.dataframe

interface DataFrame extends AbstractDataFrame {

    ArrayList as_list()

    LinkedHashMap as_map()

    LinkedHashMap getAt(IntRange i)

    LinkedHashMap getAt(List i)

    LinkedHashMap getAt(Integer i)

    ArrayList getAt(String var)

    DataFrame inner_join(DataFrame right, String... by)

    DataFrame inner_join(DataFrame right, Set by)

    DataFrame left_join(DataFrame right, String... by)

    DataFrame left_join(DataFrame right, Set by)

    DataFrame right_join(DataFrame right, String... by)

    DataFrame right_join(DataFrame right, Set by)

    DataFrame full_join(DataFrame right, String... by)

    DataFrame full_join(DataFrame right, Set by)

    DataFrame mutate(Closure closure)

    DataFrame mutate_with(Map with, Closure closure)

    Set names()

    int nrow()

    int ncol()

    DataFrame rename(Map nameMap)

    DataFrame select(String... vars)

    DataFrame select(Set vars)

    DataFrame slice(int... rows)

    DataFrame slice(IntRange rows)

    DataFrame slice(ArrayList rows)

    ArrayList pull(String var)


}