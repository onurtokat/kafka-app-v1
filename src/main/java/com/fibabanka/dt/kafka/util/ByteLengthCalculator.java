package com.fibabanka.dt.kafka.util;

public class ByteLengthCalculator {

    public static int getByteLength(int precision) {
        int arraySize = (int) Math.ceil((Math.log10(2) + precision) / Math.log10(256));
        /*
        if (precision <= 9) {//9
            arraySize = 4;
        } else if (precision >= 10 && precision <= 18) {//10 and 18
            arraySize = 8;
        } else if (precision > 18) {//18
            arraySize = 16;
        } else {
            arraySize = 16;
        }
        */
        return arraySize;
    }
}