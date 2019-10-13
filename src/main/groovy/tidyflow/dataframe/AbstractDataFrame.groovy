package tidyflow.dataframe

interface AbstractDataFrame {

    AbstractDataFrame arrange(Map par, String... by)

    AbstractDataFrame arrange(Map par, Set by)

    AbstractDataFrame full_join(AbstractDataFrame right, String... by)

    AbstractDataFrame full_join(AbstractDataFrame right, Set by)

    AbstractDataFrame mutate(Closure closure)

    AbstractDataFrame mutate_with(Map with, Closure closure)

    AbstractDataFrame rename(Map nameMap)

    AbstractDataFrame select(String... vars)

    AbstractDataFrame select(Set vars)



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