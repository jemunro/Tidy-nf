
import static tidyflow.Methods.*
import static test.SelectTests.selectTests
import static test.RenameTests.renameTests
import static test.MutateTests.mutateTests
import static test.SliceTests.sliceTests
import static test.PullTests.pullTests
import static test.JoinTests.joinTests

//df = as_df(
//    x: [1,2,3,4,5],
//    y: [5,4,3,2,1],
//    z: ['a','b','c','d','e'])

mutateTests()
pullTests()
renameTests()
selectTests()
sliceTests()
joinTests()

println "done."