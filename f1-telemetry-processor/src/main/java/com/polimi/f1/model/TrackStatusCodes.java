package com.polimi.f1.model;

// fia track status codes used across operators and output models.
public final class TrackStatusCodes {

    public static final String GREEN = "1";
    public static final String YELLOW = "2";
    public static final String SAFETY_CAR = "4";
    public static final String RED_FLAG = "5";
    public static final String VIRTUAL_SAFETY_CAR = "6";
    public static final String VSC_ENDING = "7";

    private TrackStatusCodes() {
    }

    public static String normalizeOrGreen(String status) {
        return status != null ? status : GREEN;
    }

    public static boolean isGreenOrUnknown(String status) {
        return status == null || GREEN.equals(status);
    }

    public static boolean isCaution(String status) {
        return SAFETY_CAR.equals(status)
                || VIRTUAL_SAFETY_CAR.equals(status)
                || VSC_ENDING.equals(status);
    }
}
