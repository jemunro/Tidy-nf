# Tidy-nf

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
* **select()**
    * select subset of variables and reorder them
    * see `dplyr::select()`
* **pull()**
    * extract a given variable
    * see `dplyr::pull()`
* **tidy()**
    * set names for variables. Converts List to LinkedHashMap.
    * see `magrittr::set_names()`
* **rename()**
    * rename a single variable
    * see `dplyr::rename()`
* **unname()**
    * Remove names. Converts LinkedHashMap to List
    * see `base::unname()`
* **unnest()**
    * un-nests inner lists, provide keys to unnest specific variables
    * see `tidyr::unnest()`
* **full_join()**
    * joins two 'TidyChannel' by selected variables, missing elements replaced by null
    * see `dplyr::full_join() `
* **left_join()**
    * exclude missing entries from left, replace missing entries from right channel with null
    * see `dplyr::left_join()`
* **right_join()**
    * exclude missing entries from right, replace missing entries from left channel with null
    * see `dplyr::right_join()`
* **inner_join()**
    * excluding missing entries from left and right channels
    * see `dplyr::inner_join()`
* **group_by()**
    * groups TidyChannel by selected variables
    * see `dplyr::group_by()`
* **to_group_size()**
    * returns a TidyVariable with size of groups
    * useful for specifying group_size parameter to group_by
* **arrange()**
    * sort within columns by row contents
    * see `dplyr::arrange()`
* **to_rows()**
    * collect TidyChannel into  TidyVariable, List of LinkedHashMap
* **to_columns()**
    * collect TidyChannel into  TidyVariable, LinkedHashMap of Lists
