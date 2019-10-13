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





    //List pull(String var)


}