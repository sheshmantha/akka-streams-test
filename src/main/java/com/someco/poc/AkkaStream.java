package com.someco.poc;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class AkkaStream {
    private static final int MAX_GROUP_RECORDS = 25;
    private static List<String[]> input;

    public AkkaStream() {
        input = new LinkedList<String[]>();
        input.add(new String[]{"One", "1a1", "1b1"});
        input.add(new String[]{"One", "1a2", "1b2"});
        input.add(new String[]{"Two", "2a1", "2b1"});
        input.add(new String[]{"Two", "2a2", "2b2"});
        input.add(new String[]{"Two", "2a3", "2b3"});
        input.add(new String[]{"Three", "3a1", "3b1"});
    }

    class Widget {
        private String first;
        private Map<Integer, String[]> rest;
        private int count;

        Widget(String f) {
            count = 0;
            first = f;
            rest = new HashMap<Integer, String[]>();
        }
        public Widget add(String s, String t) {
            rest.put(count++, new String[]{s, t});
            return this;
        }
        public String toString() {
            StringBuffer buf = new StringBuffer();
            buf.append(first + " [");
            rest.forEach((idx, arr) -> buf.append(idx + "->[" + arr[0] + "," + arr[1]+"] "));
            buf.append("]");
            return buf.toString();
        }
    }

    CompletionStage<Done> doGroupBy(ActorSystem system, Materializer materializer) {
        Source<String[], NotUsed> source = Source.from(input);
        return source.groupBy(MAX_GROUP_RECORDS, arr -> arr[0])
                .map(arr -> new Pair<String[], Widget>(arr, new Widget(arr[0]).add(arr[1], arr[2])))
                .reduce((l, r) -> new Pair<String[], Widget>(
                        l.first(),
                        l.second().add(r.first()[1], r.first()[2])
                ))
                .mergeSubstreams()
                .runForeach(p -> System.out.println(p.second()), materializer);
    }

    static public void main(String[] args) {
        final ActorSystem system = ActorSystem.create("AkkaStream-Test");
        final Materializer materializer = ActorMaterializer.create(system);

        AkkaStream stream = new AkkaStream();
        CompletionStage<Done> done = stream.doGroupBy(system, materializer);
        done.whenComplete((a,throwable) -> system.terminate());
    }
}
