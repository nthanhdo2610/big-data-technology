package miu.edu.bdt.assignment;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Assignment4 {
    public static void main(String[] args) {

//        Input split1 : [cherry mango olive cherry]
//                          [plum cherry banana cherry]
        List<Pair<String, Integer>> map1 = new ArrayList<>();
        map1.addAll(map("cherry mango olive cherry"));
        map1.addAll(map("plum cherry banana cherry"));

//        Input split2 : [cherry banana radish radish]
// 		                    [pear banana mango cherry]
        List<Pair<String, Integer>> map2 = new ArrayList<>();
        map2.addAll(map("cherry mango olive cherry"));
        map2.addAll(map("plum cherry banana cherry"));

//        Input split3 : [banana kiwi plum banana]
// 		                    [mango cherry kiwi banana]
        List<Pair<String, Integer>> map3 = new ArrayList<>();
        map3.addAll(map("banana kiwi plum banana"));
        map3.addAll(map("mango cherry kiwi banana"));

//        Input split4 : [apple mango pear plum]
//                          [radish kiwi banana olive]
        List<Pair<String, Integer>> map4 = new ArrayList<>();
        map4.addAll(map("apple mango pear plum"));
        map4.addAll(map("radish kiwi banana olive"));

//        Input split5 : [olive banana radish kiwi]
// 		                    [cherry kiwi olive cherry]
        List<Pair<String, Integer>> map5 = new ArrayList<>();
        map5.addAll(map("olive banana radish kiwi"));
        map5.addAll(map("cherry kiwi olive cherry"));

//        Input split6 : [banana radish plum banana]
// 		                    [olive cherry banana radish]
        List<Pair<String, Integer>> map6 = new ArrayList<>();
        map6.addAll(map("banana radish plum banana"));
        map6.addAll(map("olive cherry banana radish"));

//        map6.stream().map(Pair::getKey).sorted().forEach(s->System.out.printf("<%s, 1>%n", s));
//        System.out.println(map3.size() + map4.size() + map5.size() + map6.size());

        map1.addAll(map2);
        map1.addAll(map3);
        map1.addAll(map4);
        map1.addAll(map5);
        map1.addAll(map6);
        List<String> map = map1.stream().map(Pair::getKey).sorted().collect(Collectors.toList());

        String k = "";
        StringBuilder v = new StringBuilder();
        int c = 0;
        for(String s : map){
            if(!k.equals(s)){
                if(!k.isEmpty()){
//                    System.out.printf("<%s, [%s]>%n", k, v);
                    System.out.printf("<%s, %d>%n", k, c);
                }
                k = s;
                v = new StringBuilder("1");
                c = 1;
            } else {
                v.append(",1");
                c++;
            }
        }
    }

    static List<Pair<String, Integer>> map(String token){
        List<Pair<String, Integer>> maps = new ArrayList<>();
        String[] arr = token.split("\\s+");
        for(String s : arr){
//            System.out.printf("<%s, 1>%n", s);
            maps.add(new Pair<>(s, 1));
        }
//        System.out.println("---");
        return maps;
    }
}
