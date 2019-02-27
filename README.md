# Tidy-nf

## usage example
```groovy
import channelextra.ChannelExtra
import tidynf.TidyOperators
import static tidynf.TidyHelpers.*

ChannelExtra.enable(TidyOperators)

left = Channel.from([
    ['a', 1, '/file/path/1.bam'],
    ['b', 2, '/file/path/2.bam'],
    ['b', 3, '/file/path/3.bam'],
    ['c', 4, '/file/path/4.bam'],
    ['c', 5, '/file/path/5.bam'],
    ['c', 6, '/file/path/6.bam']])
    .set_names('id', 'value', 'file')
    .mutate { file = as_file(file) ; bai = file_ext(file, '.bai')}
    .group_by('id')
    .arrange( by:['value'] )
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
* mutate
    * add new variables or modify existing variables
    * see `dplyr::mutate()`
* select
    * select subset of variables and reorder them
    * see `dplyr::select()`
* tidy
    * set names for variables. Converts List to LinkedHashMap. Output is a 'TidyChannel'.
    * see `magrittr::set_names()`
* rename
    * rename a single variable
    * see `dplyr::rename()`
* unname
    * Remove names. Converts LinkedHashMap to List
    * see `base::uname()`
* unnest
    * unnests inner lists
    * see `tidyr::unnest()`
* full_join
    * joins two 'TidyChannel' by selected variables, missing elements replaced by null
    * see `dplyr::full_join() `
* left_join
    * as full join, excluding missing entries from right channel
    * see `dplyr::left_join()`
* left_cross
    * as left join, but emits all unique rows
* right_join
    * as full join, excluding missing entries from left channel
    * see `dplyr::left_join()`
* right_cross
    * as right_join, but emits all unique rows
* inner_join
    * as full join, exluding missing entries from left and right channels
    * see `dplyr::inner_join()`
* group_by
    * groups TidyChannel by select variables
    * see `dplyr::group_by()`
* arrange
    * sort 'rows' by grouped variables
    * see `dplyr::arrange`


* **Note:** unlike dplyr join operations, these only emit the first occurse of the 'by' tuple from the left and right 
channel. This means they will emit as soon as a match is found. The cross ops on the other hand will collect the source
data into a list and emit with each target data that matches.