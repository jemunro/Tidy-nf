package tidynf.helpers

import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allKeySetsMatch
import static tidynf.helpers.Predicates.isListOfMap
import static tidynf.helpers.DataHelpers.transpose

/*
    would need to modify all methods in ArrayList that modify the data
    and check if still valid DataFrame
 */
class ListOfMapDF extends ArrayList implements DataFrame {

    ListOfMapDF(List data) {
        super(data)

        if (!isListOfMap(data))
            throw new IllegalTypeException(
                    errMsg('ListOfMapDF', "Required List of Map\ngot: $data"))

        if (!allKeySetsMatch(data))
            throw new KeySetMismatchException(
                    errMsg('ListOfMapDF', "Required matching keysets\nfirst keyset:${data[0].keySet()}"))

    }

    MapOfListDF t(){
        transpose(this) as MapOfListDF
    }

    ListOfMapDF mutate(Closure cl){
        this.collect(cl) as ListOfMapDF
    }

    ListOfMapDF select(Collection vars) {
        if (! this.every { (it as LinkedHashMap).keySet().containsAll(vars) })
            throw new KeySetMismatchException(
                    errMsg('select', "Required matching keysets\nfirst keyset:${this[0].keySet()}"))

        this.collect { (it as LinkedHashMap).subMap(vars) } as ListOfMapDF
    }
}
