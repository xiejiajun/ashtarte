package com.github.harbby.ashtarte.example;

import com.github.harbby.ashtarte.BatchContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * pageRank 由google创始人 拉里·佩奇（Larry Page）发明.
 * <p>
 * 该算法为迭代型,且结果收敛
 * 迭代此时将影响收敛度
 */
public class PageRankTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .setParallelism(2)
            .getOrCreate();

    @Test
    public void pageRank4itersTest()
    {
        int iters = 1000;  //迭代次数
        String sparkHome = System.getenv("SPARK_HOME");

        DataSet<String> lines = mppContext.textFile(sparkHome + "/data/mllib/pagerank_data.txt");
        KvDataSet<String, Iterable<String>> links = lines.kvDataSet(s -> {
            String[] parts = s.split("\\s+");
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();

        KvDataSet<String, Double> ranks = links.mapValues(v -> 1.0);
        for (int i = 1; i <= iters; i++) {
            DataSet<Tuple2<String, Double>> contribs = links.join(ranks).values().flatMapIterator(it -> {
                Collection<String> urls = (Collection<String>) it.f1();
                Double rank = it.f2();

                long size = urls.size();
                return urls.stream().map(url -> new Tuple2<>(url, rank / size)).iterator();
            });

            ranks = KvDataSet.toKvDataSet(contribs).reduceByKey((x, y) -> x + y).mapValues(x -> 0.15 + 0.85 * x);
        }

        List<Tuple2<String, Double>> output = ranks.collect();
        output.forEach(tup -> System.out.println(String.format("%s has rank:  %s .", tup.f1(), tup.f2())));

        Map<String, Double> data = output.stream().collect(Collectors.toMap(k -> k.f1(), v -> v.f2()));
        Assert.assertEquals(data.get("1"), 1.918918918918918D, 1e-7);
        Assert.assertEquals(data.get("2"), 0.6936936936936938, 1e-7);
        Assert.assertEquals(data.get("3"), 0.6936936936936938, 1e-7);
        Assert.assertEquals(data.get("4"), 0.6936936936936938, 1e-7);
    }
}