#!/usr/bin/env nextflow

import static tidynf.TidyMethods.*
import static tidynf.TidyOps.*
import static tidynf.helpers.DataHelpers.transpose
import static tidynf.helpers.DataHelpers.arrange

tidynf()
workflow.onComplete { println 'done.' }

x = read_csv('coll.csv')
assert arrange(x) == transpose(arrange(transpose(x)))

left = Channel.from([
    ['a', 1, '/file/path/1.bam'],
    ['b', 2, '/file/path/2.bam'],
    ['b', 3, '/file/path/3.bam'],
    ['c', 4, '/file/path/4.bam'],
    ['c', 5, '/file/path/5.bam'],
    ['c', 6, '/file/path/6.bam']])
    .set_names('id', 'value', 'file')
    .mutate { file = file(file) ; bai = file(file +'.bai')}
    .group_by('id')
    .arrange('value')
    .mutate { n = value.size() }

right = Channel.from([
    ['a', 'foo'],
    ['b', 'bar'],
    ['c', 'baz'],
    ['d', 'zum']])
    .set_names('id', 'var')

left.inner_join(right, 'id')
    .unnest()
    .collect_cols()
    .arrange('id', 'value')
    .unnest()
    .subscribe_tsv('sub.tsv')
    .collect_tsv('col.tsv')
    .subscribe { println read_tsv(it).collect { it.toString() }.join('\n') }