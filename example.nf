#!/usr/bin/env nextflow

import static tidynf.TidyMethods.*
import static tidynf.TidyOps.*

tidynf()
workflow.onComplete { println 'done.' }


left = Channel.from([
    ['a', 1, '/file/path/1.bam'],
    ['b', 2, '/file/path/2.bam'],
    ['b', 3, '/file/path/3.bam'],
    ['c', 4, '/file/path/4.bam'],
    ['c', 5, '/file/path/5.bam'],
    ['c', 6, '/file/path/6.bam']])
    .set_names('id', 'value', 'file')
    .mutate { file = file(file) ; bai = file(file +'.bai'); x = y + 1}
    .group_by('id')
    .arrange('value')
    .mutate { n = value.size() }

right = Channel.from([
    ['a', 'foo'],
    ['b', 'bar'],
    ['c', 'baz']])
    .set_names('id', 'var')

left.full_join(right, 'id')
    .unnest()
    .subscribe_tsv('sub.tsv')
    .collect_csv('col.tsv')
    .subscribe { println it.toFile().text }