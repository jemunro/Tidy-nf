
import static tidyflow.TidyMethods.*
import static test.SelectTests.selectTests
import static test.RenameTests.renameTests


df = as_df(
    x: [1,2,3,4,5],
    y: [5,4,3,2,1],
    z: ['a','b','c','d','e'])

selectTests()
renameTests()

println "done."