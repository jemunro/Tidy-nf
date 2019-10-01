package tidynf.dataframe


import tidynf.exception.CollectionSizeMismatchException
import tidynf.exception.IllegalTypeException
import tidynf.exception.TypeMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allAreListOfSameType
import static tidynf.helpers.Predicates.allAreSameSize
import static tidynf.helpers.Predicates.isMapOfList
import static DataFrameMethods.transpose

class MapOfListDataFrame implements DataFrame {

    private LinkedHashMap data
    private LinkedHashSet keySet

    MapOfListDataFrame(LinkedHashMap data) {

        if (!isMapOfList(data))
            throw new IllegalTypeException(
                    errMsg('MapOfListDF', "Required List of Map\ngot: $data"))

        if (!allAreSameSize(data.values()))
            throw new CollectionSizeMismatchException(
                    errMsg('MapOfListDF', "Required all lists to be same size"))

        if (!allAreListOfSameType(data))
            throw new TypeMismatchException(
                    errMsg('MapOfListDF', "Required matching data types for each variable"))

        this.data = data
        this.keySet = data.keySet()
    }

    ArrayList as_list() {
        transpose(this.data)
    }

    LinkedHashMap as_map() {
        this.data
    }

    ListOfMapDataFrame t() {
        transpose(this.data) as ListOfMapDataFrame
    }

    MapOfListDataFrame mutate(Closure cl) {
        t().mutate(cl).t()
    }

    MapOfListDataFrame select(String... vars) {
        select(vars as Collection)
    }

    MapOfListDataFrame select(Collection vars) {
        this.data.subMap(vars) as MapOfListDataFrame
    }
}
