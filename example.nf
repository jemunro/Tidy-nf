#!/usr/bin/env nextflow
import static tidynf.TidyMethods.*

tidynf()


x = data_frame(read_csv('coll.csv'))

println '\n' +  x.as_list().join('\n')

y = x.transpose().select('id', 'value', 'var', 'n')
    .arrange().transpose()

println y

println y.transpose()

//workflow.onComplete { println 'done.' }
//
//left = Channel.from([
//    ['a', 1, '/file/path/1.bam'],
//    ['b', 2, '/file/path/2.bam'],
//    ['b', 3, '/file/path/3.bam'],
//    ['c', 4, '/file/path/4.bam'],
//    ['c', 5, '/file/path/5.bam'],
//    ['c', 6, '/file/path/6.bam']])
//    .set_names('id', 'value', 'bam')
//    .mutate { bam = file(bam) }
//    .group_by('id')
//    .mutate { n = value.size() }
//    .unnest()
//
//right = Channel.from([
//    ['a', 'foo'],
//    ['b', 'bar'],
//    ['c', 'baz'],
//    ['d', 'zum']])
//    .set_names('id', 'var')
//
//left.inner_join(right, 'id')
//    .collect_cols()
//    .arrange('id', 'value')
//    .unnest()
//    .select('id', 'value', 'var', 'n', 'bam')
//    .collect_rows()
//    .subscribe { println it.collect { it.toString() }.join('\n') }