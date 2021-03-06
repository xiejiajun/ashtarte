package com.github.harbby.ashtarte.example;

import com.github.harbby.ashtarte.BatchContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public class SparkGroupByDemo
{
    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().setParallelism(1).getOrCreate();
        String sparkHome = System.getenv("SPARK_HOME");
        DataSet<String> ds = mppContext.textFile(sparkHome + "/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        KvDataSet<String, Long> kvDataSet = worlds.kvDataSet(x -> new Tuple2<>(x, 1L));
        KvDataSet<String, Long> worldCounts = kvDataSet.partitionBy(2).reduceByKey(Long::sum);

        KvDataSet<String, Long> worldCounts2 = worldCounts
                .rePartition(4)
                .mapKeys(k -> k.substring(0, 1))
                .groupByKey()
                .mapValues(input -> Iterators.size(input.iterator()));

        worldCounts2.foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
