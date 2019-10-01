package tidynf.dataframe

interface AbstractDataFrame {

    AbstractDataFrame mutate(Closure closure)

    AbstractDataFrame select(Collection variables)

//    List pull(String variable)

//    DataFrame left_join(DataFrame dataFrame)
//
//    DataFrame semi_join(DataFrame dataFrame)
//
//    DataFrame inner_join(DataFrame dataFrame)
//
//    DataFrame full_join(DataFrame dataFrame)

}