#!/usr/bin/env nextflow
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
    .tidy('id', 'value', 'file')
    .mutate { file = as_file(file) ; bai = file_ext(file, '.bai')}
    .group_by('id')
    .arrange( by:['value'] )
    .mutate { n = value.size() }

right = Channel.from([
    ['a', 'foo'],
    ['b', 'bar'],
    ['c', 'baz']])
    .tidy('id', 'var')

left.full_join(right, 'id')
    .subscribe { println it }
