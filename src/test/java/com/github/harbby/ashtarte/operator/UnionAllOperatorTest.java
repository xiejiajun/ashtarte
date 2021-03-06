package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.BatchContext;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UnionAllOperatorTest
{
    private final BatchContext mppContext = BatchContext.builder().setParallelism(1).getOrCreate();

    @Test
    public void kvDsUnionAllTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).reduceByKey(Integer::sum);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 21)
        ), 2).reduceByKey(Integer::sum);
        //.distinct();

        //ageDs.print();
        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);

        Assert.assertEquals(out.numPartitions(), ds1.numPartitions() + ds2.numPartitions());

        List<Tuple2<String, Integer>> data = out.collect();
        Assert.assertEquals(data,
                Arrays.asList(Tuple2.of("hp", 20),
                        Tuple2.of("hp1", 19),
                        Tuple2.of("hp2", 21)));
    }

    @Test
    public void unionAllOneShuffleTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18)
        )).mapValues(x -> x);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 21)
        ), 2).reduceByKey(Integer::sum).mapValues(x -> x);
        //.distinct();

        //ageDs.print();
        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);

        Assert.assertEquals(out.numPartitions(), ds1.numPartitions() + ds2.numPartitions());

        List<Tuple2<String, Integer>> data = out.collect();
        Assert.assertEquals(data,
                Arrays.asList(Tuple2.of("hp", 20),
                        Tuple2.of("hp1", 19),
                        Tuple2.of("hp2", 21)));
    }

    @Test
    public void unionAllNoShuffleTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18)
        )).mapValues(x -> x);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 21)
        ), 2).mapValues(x -> x);
        //.distinct();

        //ageDs.print();
        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);

        Assert.assertEquals(out.numPartitions(), ds1.numPartitions() + ds2.numPartitions());

        List<Tuple2<String, Integer>> data = out.collect();
        Assert.assertEquals(data,
                Arrays.asList(Tuple2.of("hp", 20),
                        Tuple2.of("hp1", 19),
                        Tuple2.of("hp2", 21)));
    }
}