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