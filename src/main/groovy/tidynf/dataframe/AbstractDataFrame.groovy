package tidynf.dataframe

interface AbstractDataFrame {

    AbstractDataFrame mutate(Closure closure)

    AbstractDataFrame select(String... vars)

    AbstractDataFrame select(Set vars)

    AbstractDataFrame arrange(Map par, String... by)

    AbstractDataFrame arrange(Map par, Set by)

    AbstractDataFrame transpose()

//    List pull(String variable)

//    DataFrame left_join(DataFrame dataFrame)
//
//    DataFrame right_join(DataFrame dataFrame)
//
//    DataFrame inner_join(DataFrame dataFrame)
//
//    DataFrame full_join(DataFrame dataFrame)
//
//    DataFrame anti_join(DataFrame dataFrame)

}