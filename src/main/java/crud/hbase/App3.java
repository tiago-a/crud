package crud.hbase;

import java.util.Scanner;

public class App3 {

	public static void main(String[] args) {
		System.out.println("digita ai!");
		Scanner scanner = new Scanner(System.in);
		String nextLine = scanner.nextLine();
		System.out.println(Long.MAX_VALUE);
		for (int i = 0; i < 100; i++) {
			long currentTimeMillis = System.currentTimeMillis();
			System.out.println("current: " + currentTimeMillis);
			System.out.println("max - current: " + (Long.MAX_VALUE - currentTimeMillis));
			System.out.println();
		}
	}

}
