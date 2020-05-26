package com.practice;

public class RotateArray {

    public static void main(String a[]){
        int input[]= new int[]{1,2,3,5,4,15,6,8};
        int order=3;

        for(int i=0;i<order;i++){
            for(int j=input.length -1; j>0;j--){
                //System.out.println("j "+j);
                int temp = input[j];
                input[j] = input[j-1];
                input[j-1] = temp;
            }
        }

        for(int i :input)
        {
            System.out.println(i);
        }

    }
}
