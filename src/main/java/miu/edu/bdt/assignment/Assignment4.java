package miu.edu.bdt.assignment;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Assignment4 {
    public static void main(String[] args) {

        System.out.println("map1");
        List<Pair<String, Integer>> map1 = new ArrayList<>();
        map1.addAll(mapInput("cherry mango olive cherry"));
        map1.addAll(mapInput("plum cherry banana cherry"));

        System.out.println("map2");
        List<Pair<String, Integer>> map2 = new ArrayList<>();
        map2.addAll(mapInput("cherry banana radish radish"));
        map2.addAll(mapInput("carrot banana mango cherry"));
        map1.addAll(map2);
        sorted(map1);


        System.out.println("map3");
        List<Pair<String, Integer>> map3 = new ArrayList<>();
        map3.addAll(mapInput("banana kiwi plum banana"));
        map3.addAll(mapInput("mango cherry kiwi banana"));

        System.out.println("map4");
        List<Pair<String, Integer>> map4 = new ArrayList<>();
        map4.addAll(mapInput("apple mango carrot plum"));
        map4.addAll(mapInput("radish kiwi banana olive"));
        map3.addAll(map4);
        sorted(map3);

        System.out.println("map5");
        List<Pair<String, Integer>> map5 = new ArrayList<>();
        map5.addAll(mapInput("olive banana radish kiwi"));
        map5.addAll(mapInput("cherry kiwi olive cherry"));
        System.out.println("combiner output");
        List<Pair<String, Integer>> combinerOutput5 = mapOutput(map5);

        System.out.println("map6");
        List<Pair<String, Integer>> map6 = new ArrayList<>();
        map6.addAll(mapInput("banana radish plum banana"));
        map6.addAll(mapInput("olive cherry banana radish"));
        System.out.println("combiner output");
        List<Pair<String, Integer>> combinerOutput6 = mapOutput(map6);

        System.out.println("reducer input");
        combinerOutput5.addAll(combinerOutput6);
        sorted(combinerOutput5);

    }

    static List<Pair<String, Integer>> mapInput(String token){
        List<Pair<String, Integer>> maps = new ArrayList<>();
        String[] arr = token.split("\\s+");
        for(String s : arr){
//            System.out.printf("<%s, 1>%n", s);
            maps.add(new Pair<>(s, 1));
        }
//        System.out.println("---");
        return maps;
    }

    static List<Pair<String, Integer>> mapOutput(List<Pair<String, Integer>> input){

        List<Pair<String, Integer>> output = new ArrayList<>();
        List<String> mapSorted = input.stream().map(Pair::getKey).sorted().collect(Collectors.toList());

        String k = "";
        int c = 0;
        for(String s : mapSorted){
            if(!k.equals(s)){
                if(!k.isEmpty()){
//                    System.out.printf("<%s, %d>%n", k, c);
                    output.add(new Pair<>(k, c));
                }
                k = s;
                c = 1;
            } else {
                c++;
            }
        }
//        System.out.println("---");
        return output;
    }

    static void sorted(List<Pair<String, Integer>> input){
        input.stream().map(Pair::getKey).distinct().sorted().collect(Collectors.toList())
                .forEach(k-> System.out.printf("<%s, []>%n", k));
    }
}
