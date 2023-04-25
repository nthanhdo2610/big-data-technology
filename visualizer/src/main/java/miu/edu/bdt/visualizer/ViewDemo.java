package miu.edu.bdt.visualizer;

public class ViewDemo {

    private static final SparkService sparkService = SparkService.getInstance();

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            sparkService.updateView();
            Thread.sleep(5000);
        }
    }
}
