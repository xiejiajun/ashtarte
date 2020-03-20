package com.github.harbby.ashtarte.example;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Collection;
import java.util.List;

public class PageRank {

    public static void main(String[] args) {
        MppContext mppContext = MppContext.builder()
                .setParallelism(2)
                .getOrCreate();
        int iters = 4;  //迭代次数
        String sparkHome = System.getenv("SPARK_HOME");

        DataSet<String> lines = mppContext.textFile(sparkHome + "/data/mllib/pagerank_data.txt");

        KvDataSet<String, Iterable<String>> links = lines.kvDataSet(s -> {
            String[] parts = s.split("\\s+");
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();

        KvDataSet<String, Double> ranks = links.mapValues(v -> 1.0);

        for (int i = 1; i <= iters; i++) {
            KvDataSet<String, Double> contribs = links.join(ranks).values().flatMapIterator(it -> {
                Collection<String> urls = (Collection<String>) it.f1();
                Double rank = it.f2();

                long size = urls.size();
                return urls.stream().map(url -> new Tuple2<>(url, rank / size)).iterator();
            }).kvDataSet(x -> x);
            ranks = contribs.reduceByKey((x, y) -> x + y).mapValues(x -> 0.15 + 0.85 * x);
        }

        List<Tuple2<String, Double>> output = ranks.collect();
        output.forEach(tup -> System.out.println(String.format("%s has rank:  %s .", tup.f1(), tup.f2())));
    }
}