
import static tidyflow.TidyMethods.*
import static test.SelectTests.selectTests
import static test.RenameTests.renameTests
import static test.MutateTests.mutateTests


df = as_df(
    x: [1,2,3,4,5],
    y: [5,4,3,2,1],
    z: ['a','b','c','d','e'])

println df.mutate { x = x + 1 ; w = y - 1}

selectTests()
renameTests()
mutateTests()

println "done."