package tidynf.helpers

import tidynf.exception.CollectionSizeMismatchException
import tidynf.exception.IllegalTypeException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allAreSameSize
import static tidynf.helpers.Predicates.isMapOfList
import static tidynf.helpers.DataHelpers.transpose

class MapOfListDF implements DataFrame {

    private LinkedHashMap data
    private LinkedHashSet keySet

    MapOfListDF(LinkedHashMap data) {

        if (!isMapOfList(data))
            throw new IllegalTypeException(
                    errMsg('ListOfMapDF', "Required List of Map\ngot: $data"))

        if (!allAreSameSize(data.values()))
            throw new CollectionSizeMismatchException(errMsg("Required all lists to be same size"))

        this.data = data
        this.keySet = data.keySet()
    }

    ArrayList as_list() {
        t().as_list()
    }

    LinkedHashMap as_map() {
        this.data
    }

    ListOfMapDF t(){
        transpose(this.data) as ListOfMapDF
    }

    MapOfListDF mutate(Closure cl){
        this.data.collect(cl) as MapOfListDF
    }

    MapOfListDF select(Collection vars) {
        this.data.subMap(vars) as MapOfListDF
    }
}
