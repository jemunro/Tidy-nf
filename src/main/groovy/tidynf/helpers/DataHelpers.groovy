package tidynf.helpers

import tidynf.exception.CollectionSizeMismatchException
import tidynf.exception.IllegalTypeException
import tidynf.exception.KeySetMismatchException

import static tidynf.exception.Message.errMsg
import static tidynf.helpers.Predicates.allAreSameSize
import static tidynf.helpers.Predicates.allKeySetsMatch
import static tidynf.helpers.Predicates.isListOfMap
import static tidynf.helpers.Predicates.isMapOfList

class DataHelpers {

    static ArrayList arrange(ArrayList data, boolean reverse = false) {
        transpose(arrange(transpose(data), reverse))
    }

    static ArrayList arrange(ArrayList data, LinkedHashSet by, boolean reverse = false) {
        transpose(arrange(transpose(data), by, reverse))
    }

    static LinkedHashMap arrange(LinkedHashMap data, boolean reverse = false) {
        arrange(data, data.keySet() as LinkedHashSet, reverse)
    }

    static LinkedHashMap arrange(LinkedHashMap data, LinkedHashSet by, boolean reverse = false) {

        if (!isMapOfList(data))
            throw new IllegalTypeException(errMsg("Required Map of List\ngot: $data"))

        if (!allAreSameSize(data.values()))
            throw new CollectionSizeMismatchException(errMsg("Required all lists to be same size"))

        if (!data.keySet().containsAll(by))
            throw new KeySetMismatchException(errMsg("by keyset not all present in keyset\n" +
                    "by keyset: $by, keyset: ${data.keySet()}"))

        final LinkedHashSet byAt = by + data.keySet()

        byAt.collect { data[it] }.transpose()
                .collect { [it.take(by.size()), it.takeRight(it.size() - by.size())] }
                .sort { l1, l2 ->
                    [l1[0], l2[0]].transpose()
                            .find { e1, e2 -> e1 != e2 }
                            .with { it ? it[0] <=> it[1] : 0 }
                }
                .with { reverse ? it.reverse() : it }
                .collect { it[0] + it[1] }
                .transpose()
                .withIndex()
                .collectEntries { item, i -> [(byAt[i]): item] }
                .with { data.keySet().collectEntries { k -> [(k): it[k]] } as LinkedHashMap }
    }

    static LinkedHashMap transpose(ArrayList data) {

        if (!isListOfMap(data))
            throw new IllegalTypeException(errMsg("Required List of Map\ngot: $data"))

        if (!allKeySetsMatch(data))
            throw new KeySetMismatchException(errMsg("Required all keysets to match"))

        LinkedHashSet keySet = (data[0] as LinkedHashMap).keySet()

        keySet.collectEntries { k -> [(k): data.collect { (it as LinkedHashMap)[k] }] } as LinkedHashMap

    }

    static ArrayList transpose(LinkedHashMap data) {

        if (!isMapOfList(data))
            throw new IllegalTypeException(errMsg("Required Map of List\ngot: $data"))

        if (!allAreSameSize(data.values()))
            throw new CollectionSizeMismatchException(errMsg("Required all lists to be same size"))

        int n = data[data.keySet()[0]].size()

        (0..<n).collect { i -> data.keySet().collectEntries { k -> [(k): data[k][i]] } }
    }
}
