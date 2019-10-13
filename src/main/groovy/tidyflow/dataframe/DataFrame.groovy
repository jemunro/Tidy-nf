package tidyflow.dataframe

interface DataFrame extends AbstractDataFrame {

    ArrayList as_list()

    LinkedHashMap as_map()

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

    //List pull(String var)


}