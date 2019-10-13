package tidyflow.dataframe

interface DataFrame extends AbstractDataFrame {

    ArrayList as_list()

    LinkedHashMap as_map()

    int nrow()

    int ncol()

    Set names()

    DataFrame select(String... vars)

    DataFrame select(Set vars)

    DataFrame rename(Map nameMap)

    //List pull(String var)


}