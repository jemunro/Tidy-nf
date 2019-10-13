package tidyflow.dataframe

interface AbstractDataFrame {

    AbstractDataFrame mutate(Closure closure)

    AbstractDataFrame select(String... vars)

    AbstractDataFrame select(Set vars)

    AbstractDataFrame arrange(Map par, String... by)

    AbstractDataFrame arrange(Map par, Set by)

    AbstractDataFrame transpose()

    AbstractDataFrame full_join(AbstractDataFrame right, String... by)

    AbstractDataFrame full_join(AbstractDataFrame right, Set by)

    DataFrame rename(Map nameMap)

//    Object names()
//
//    Object as_list()
//
//    Object as_map()


//    AbstractDataFrame rename(Map namesFromTo)

//    AbstractDataFrame group_by(DataFrame dataFrame)
//
//    AbstractDataFrame group_by(DataFrame dataFrame)

//    AbstractDataFrame left_join(DataFrame dataFrame)
//
//    AbstractDataFrame right_join(DataFrame dataFrame)
//
//    AbstractDataFrame inner_join(DataFrame dataFrame)
//

//
//    AbstractDataFrame anti_join(DataFrame dataFrame)

//    AbstractDataFrame semi_join(DataFrame dataFrame)

}