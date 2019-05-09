package com.alicloud.openservices.tablestore.timeline2.utils;

import com.alicloud.openservices.tablestore.timeline2.TimelineException;

public final class Preconditions {
    private Preconditions() {
    }

    public static void checkArgument(boolean expression) {
        if(!expression) {
            throw new TimelineException();
        }
    }

    public static void checkArgument(boolean expression, Object errorMessage) {
        if(!expression) {
            throw new TimelineException(String.valueOf(errorMessage));
        }
    }

    public static void checkArgument(boolean expression, String errorMessageTemplate, Object... errorMessageArgs) {
        if(!expression) {
            throw new TimelineException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static <T> T checkNotNull(T reference) {
        if(reference == null) {
            throw new TimelineException();
        } else {
            return reference;
        }
    }

    public static <T> T checkNotNull(T reference, Object errorMessage) {
        if(reference == null) {
            throw new TimelineException(String.valueOf(errorMessage));
        } else {
            return reference;
        }
    }

    public static <T> T checkNotNull(T reference, String errorMessageTemplate, Object... errorMessageArgs) {
        if(reference == null) {
            throw new TimelineException(format(errorMessageTemplate, errorMessageArgs));
        } else {
            return reference;
        }
    }

    public static void checkNotEmptyString(String reference, Object errorMessage) {
        if("".equals(reference)) {
            throw new TimelineException(String.valueOf(errorMessage));
        }
    }

    public static void checkStringNotNullAndEmpty(String reference, Object errorMessage) {
        if(reference == null || reference.equals("")) {
            throw new TimelineException(String.valueOf(errorMessage));
        }
    }

    static String format(String template, Object... args) {
        template = String.valueOf(template);
        StringBuilder builder = new StringBuilder(template.length() + 16 * args.length);
        int templateStart = 0;

        int i;
        int placeholderStart;
        for(i = 0; i < args.length; templateStart = placeholderStart + 2) {
            placeholderStart = template.indexOf("%s", templateStart);
            if(placeholderStart == -1) {
                break;
            }

            builder.append(template.substring(templateStart, placeholderStart));
            builder.append(args[i++]);
        }

        builder.append(template.substring(templateStart));
        if(i < args.length) {
            builder.append(" [");
            builder.append(args[i++]);

            while(i < args.length) {
                builder.append(", ");
                builder.append(args[i++]);
            }

            builder.append("]");
        }

        return builder.toString();
    }
}
