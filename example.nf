#!/usr/bin/env nextflow

import static tidynf.TidyMethods.*
tidynf()

left = Channel.from([
    ['a', 1, '/file/path/1.bam'],
    ['b', 2, '/file/path/2.bam'],
    ['b', 3, '/file/path/3.bam'],
    ['c', 4, '/file/path/4.bam'],
    ['c', 5, '/file/path/5.bam'],
    ['c', 6, 'test.tsv']])
    .set_names('id', 'value', 'bam')
    .collect_cols()
    .mutate { bam = bam.collect { file(it) }; size = bam.collect { file_size(it, 'B') }.sum() }
    .subscribe { println it }

//
//left = Channel.from([
//    ['a', 1, '/file/path/1.bam'],
//    ['b', 2, '/file/path/2.bam'],
//    ['b', 3, '/file/path/3.bam'],
//    ['c', 4, '/file/path/4.bam'],
//    ['c', 5, '/file/path/5.bam'],
//    ['c', 6, '/file/path/6.bam']])
//    .set_names('id', 'value', 'file')
//    .mutate { file = file(file) ; bai = file(file +'.bai') }
//    .group_by('id')
//    .arrange('value')
//    .mutate { n = value.size() }
//
//right = Channel.from([
//    ['a', 'foo'],
//    ['b', 'bar'],
//    ['c', 'baz']])
//    .set_names('id', 'var')
//
//left.full_join(right, 'id')
//    .unnest()
//    .toList()
//    .subscribe { println toJson(it) }
