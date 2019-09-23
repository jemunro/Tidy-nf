# Tidy-nf
Channel operators for Nextflow based on dataframe manipulation in the
tidyverse packages from the R programming language.

## usage example
```groovy
import static tidynf.TidyMethods.*
tidynf()

left = Channel.from([
    ['a', 1, '/file/path/1.bam'],
    ['b', 2, '/file/path/2.bam'],
    ['b', 3, '/file/path/3.bam'],
    ['c', 4, '/file/path/4.bam'],
    ['c', 5, '/file/path/5.bam'],
    ['c', 6, '/file/path/6.bam']])
    .set_names('id', 'value', 'file')
    .mutate { file = as_file(file) ; bai = file_ext(file, '.bai') }
    .group_by('id')
    .arrange('value')
    .mutate { n = value.size() }

right = Channel.from([
    ['a', 'foo'],
    ['b', 'bar'],
    ['c', 'baz']])
    .set_names('id', 'var')

left.full_join(right, 'id')
    .subscribe { println it }

```

```console
N E X T F L O W  ~  version 19.01.0
Launching `example.nf` [dreamy_jennings] - revision: 6ec6ccc094
[id:a, value:[1], file:[/file/path/1.bam], bai:[/file/path/1.bam.bai], n:1, var:foo]
[id:b, value:[2, 3], file:[/file/path/2.bam, /file/path/3.bam], bai:[/file/path/2.bam.bai, /file/path/3.bam.bai], n:2, var:bar]
[id:c, value:[4, 5, 6], file:[/file/path/4.bam, /file/path/5.bam, /file/path/6.bam], bai:[/file/path/4.bam.bai, /file/path/5.bam.bai, /file/path/6.bam.bai], n:3, var:baz]
```

## TidyOperators
* **mutate()**
    * add new variables or modify existing variables
    * see `dplyr::mutate()`
    * differences:
        - variables from global environment cannot be used unless included explicitly
        - mutate performed with groovy closure instead of R function
    * e.g. `channel.mutate (var: var) { x = x + var }`
* **select()**
    * select subset of variables and reorder them
    * see `dplyr::select()`
    * differences:
        - variables to be selected must be given as strings
        - no negative selection with `-`
    * e.g. `channel.select('x', 'y', 'z')`
* **pull()**
    * extract a given variable
    * see `dplyr::pull()`
    * e.g. `channel.pull('x')`
* **set_names()**
    * set names for variables. Converts List to LinkedHashMap.
    * see `magrittr::set_names()`
    * e.g. `channel.set_names('x', 'y', 'z')`
* **rename()**
    * rename a variable
    * see `dplyr::rename()`
    * differences:
            - only a single variable may be renames
            - not given as a formula
    * e.g. `channel.rename('old_name', 'new_name')`
* **unname()**
    * Remove names. Converts LinkedHashMap to List
    * see `base::unname()`
* **unnest()**
    * un-nests inner lists, such as those produce by `group_by`,
     provide keys to unnest specific variables
    * see `tidyr::unnest()`
    * e.g. `channel.unnest()` or `channel.unnest('x', 'y')`
* **left_join()**, **right_join()**, **full_join()**, **inner_join()**
    * joins two 'TidyChannel' by selected variables, missing elements replaced by null
    * differences:
        - overlapping variable names only permitted for names in `by`
    * see `dplyr::left_join()`, `dplyr::right_join()`, `dplyr::full_join()`, `dplyr::inner_join()`
    * e.g. `left.full_join(right, 'x')`
* **group_by()**
    * group by selected variables
    * see `dplyr::group_by()`
    * differences:
        - grouping is more explicit, with non-grouping variables collected into lists
    * e.g. `channel.group_by('x', 'y')`
* **arrange()**
    * sort rows by row contents
    * see `dplyr::arrange()`
    * differences:
        - only works on collected list variables, i.e. those produced by `group_by`
* **collect_rows()**
    * collect Channel into Variable, List of LinkedHashMap
    * e.g. channel.collect_rows()
* **collect_cols()**
    * collect Channel into Variable, LinkedHashMap of List
    * e.g. channel.collect_cols()
