package tidynf.helpers

import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allKeySetsMatch
import static tidynf.helpers.Predicates.isListOfMap
import static tidynf.helpers.DataHelpers.transpose

class ListOfMapDF implements DataFrame {

    private ArrayList data
    private LinkedHashSet keySet

    ListOfMapDF(List data) {

        if (!isListOfMap(data))
            throw new IllegalTypeException(
                    errMsg('ListOfMapDF', "Required List of Map\ngot: $data"))

        if (!allKeySetsMatch(data))
            throw new KeySetMismatchException(
                    errMsg('ListOfMapDF', "Required matching keysets\nfirst keyset:${data[0].keySet()}"))

        this.data = data
        this.keySet = (data[0] as LinkedHashMap).keySet()
    }

    ArrayList as_list() {
        this.data
    }

    LinkedHashMap as_map() {
        t().as_map()
    }

    MapOfListDF t(){
        transpose(this.data) as MapOfListDF
    }

    ListOfMapDF mutate(Closure cl){
        this.data.collect(cl) as ListOfMapDF
    }

    ListOfMapDF select(Collection vars) {
        this.data.collect { (it as LinkedHashMap).subMap(vars) } as ListOfMapDF
    }
}
