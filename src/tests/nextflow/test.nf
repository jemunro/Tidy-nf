#!/usr/bin/env nextflow

import tidyflow.dataframe.DataFrame
import static tidyflow.Methods.*

workflow.onComplete { println "done." }

df = data_frame(
    x: [1,2,3,4,5],
    y: [5,4,3,2,1],
    z: ['a','b','c','d','e'])
ch =
//ch.subscribe { println it }

dfdf = as_df_df(Channel.from(df.as_list()))
dfdf.mutate { x = x + 1}.emit().toList().subscribe { println it }

