package miu.edu.bdt.visualizer;

import java.util.Scanner;

public class SparkSQLAnalysis {

    private static final SparkService sparkService = SparkService.getInstance();
//    private static final Logger logger = LoggerFactory.getLogger(SparkSQLAnalysis.class);

    public static void main(String[] args) throws InterruptedException {
    	try (Scanner scanner = new Scanner(System.in)) {
			while (true) {
				showMenu();
				String option = scanner.nextLine();

				switch (option) {
					case "1":
						sparkService.getWeatherIOWA();
						break;
					case "2":
						sparkService.getLast7DaysAvgTempByArea();
						break;
					case "3":
						sparkService.getHotAreaWithTempGreaterThan83();
						break;
					case "0":
						System.exit(1);
					default:
				}
			}
		}
    }
    
    private static void showMenu() {
		System.out.println("Welcome to The Weather Report Application");
		System.out.println("Please select the option:");
		System.out.println("Enter number '1' to get the Weather in IOWA");
		System.out.println("Enter number '2' to get the Average Temperature by Area");
		System.out.println("Enter number '3' to get the hot Area");
		System.out.println("Enter number '0' to stop program");
	}
}
