package org.embulk.filter.typecast;

public class JsonPathUtil
{
    private JsonPathUtil() {}

    public static boolean isProbablyJsonPath(String jsonPath) {
        return jsonPath.startsWith("$.") || jsonPath.startsWith("$[");
    }

}
