package tidynf.helpers

import tidynf.exception.CollectionSizeMismatchException
import tidynf.exception.IllegalTypeException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allAreSameSize
import static tidynf.helpers.Predicates.isMapOfList
import static tidynf.helpers.DataHelpers.transpose

class MapOfListDF extends LinkedHashMap implements DataFrame{

    MapOfListDF(LinkedHashMap data) {
        super(data)

        if (!isMapOfList(data))
            throw new IllegalTypeException(
                    errMsg('ListOfMapDF', "Required List of Map\ngot: $data"))

        if (!allAreSameSize(data.values()))
            throw new CollectionSizeMismatchException(errMsg("Required all lists to be same size"))
    }

    ListOfMapDF t(){
        transpose(this) as ListOfMapDF
    }

    MapOfListDF mutate(Closure cl){
        this.collect(cl) as MapOfListDF
    }
}
