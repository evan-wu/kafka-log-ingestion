package com.tazhi.log.ingestion.util;

public class Assert {

    public static void notEmpty(String str, String message) {
        if (str == null || str.length() == 0)
            throw new IllegalArgumentException(message);
    }
    
}
