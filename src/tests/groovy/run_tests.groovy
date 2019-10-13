
import static tidyflow.TidyMethods.*
import static test.SelectTests.selectTests
import static test.RenameTests.renameTests
import static test.MutateTests.mutateTests
import static test.SliceTests.sliceTests


df = as_df(
    x: [1,2,3,4,5],
    y: [5,4,3,2,1],
    z: ['a','b','c','d','e'])

println df.slice(0,2,4)

selectTests()
renameTests()
mutateTests()
sliceTests()

println "done."