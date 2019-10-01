package tidynf.dataframe


import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException
import tidynf.exception.TypeMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allKeySetsMatch
import static tidynf.helpers.Predicates.isListOfMap
import static DataFrameMethods.transpose
import static tidynf.helpers.Predicates.isListOfMapOfSameType

class ListOfMapDataFrame implements DataFrame {

    private ArrayList data
    private LinkedHashSet keySet

    ListOfMapDataFrame(List data) {

        if (!isListOfMap(data))
            throw new IllegalTypeException(
                    errMsg('ListOfMapDF', "Required List of Map\ngot: $data"))

        if (!allKeySetsMatch(data))
            throw new KeySetMismatchException(
                    errMsg('ListOfMapDF', "Required matching keysets\nfirst keyset:${data[0].keySet()}"))

        if (!isListOfMapOfSameType(data))
            throw new TypeMismatchException(
                    errMsg('ListOfMapDF', "Required matching data types for each variable"))

        this.data = data
        this.keySet = (data[0] as LinkedHashMap).keySet()
    }

    ListOfMapDataFrame(LinkedHashMap data) {
        this.data = [data]
        this.keySet = data.keySet()
    }

    ArrayList as_list() {
        this.data
    }

    LinkedHashMap as_map() {
        transpose(this.data)
    }

    MapOfListDataFrame t() {
        transpose(this.data) as MapOfListDataFrame
    }

    ListOfMapDataFrame mutate(Closure cl) {
        this.data.collect(cl) as ListOfMapDataFrame
    }

    ListOfMapDataFrame select(String... vars) {
        select(vars as Collection)
    }

    ListOfMapDataFrame select(Collection vars) {
        this.data.collect { (it as LinkedHashMap).subMap(vars) } as ListOfMapDataFrame
    }
}
