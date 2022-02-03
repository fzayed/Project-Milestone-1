package com.company;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello world");
        int count = 0;
        try{
            while (true){
                Thread.sleep(1000);
                System.out.println("Still running. Iteration " + count++);
            }
        } catch(InterruptedException e){
            e.printStackTrace();
        }
    }
}
