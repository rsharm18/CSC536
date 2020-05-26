package com.practice;

public class StringReversal {

    public static void main(String a[]) {
        String data = "the sky is blue";
        System.out.println("Input Data : "+data);
        reverseData(data.split(" "));
    }

    private static void reverseData(String a[]) {
        StringBuffer buf = new StringBuffer();

        for (int  j = a.length -1; j>=0;  j--) {
            buf.append(a[j]);
            if(j!=0)
            buf.append(" ");
        }
      System.out.println(" Output : "+buf);
    }
}
