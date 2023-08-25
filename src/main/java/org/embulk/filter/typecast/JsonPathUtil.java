package org.embulk.filter.typecast;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.internal.Path;
import com.jayway.jsonpath.internal.path.CompiledPath;
import com.jayway.jsonpath.internal.path.PathCompiler;
import com.jayway.jsonpath.internal.path.PathToken;
import org.embulk.config.ConfigException;

public class JsonPathUtil
{
    private JsonPathUtil() {}

    public static boolean isProbablyJsonPath(String jsonPath) {
        return jsonPath.startsWith("$.") || jsonPath.startsWith("$[");
    }

    public static String getColumnName(String jsonPath)
    {
        Path compiledPath;
        try {
            compiledPath = PathCompiler.compile(jsonPath);
        }
        catch (InvalidPathException e) {
            throw new ConfigException(String.format("jsonpath %s, %s", jsonPath, e.getMessage()));
        }
        PathToken pathToken = ((CompiledPath) compiledPath).getRoot();
        pathToken = pathToken.getNext(); // skip $
        String fragment;
        if (pathToken.getTokenCount() == 1) {
            fragment = pathToken.toString();
        } else {
            fragment = pathToken.toString().replace(pathToken.getNext().toString(), "");
        }
        return fragment.substring(2, fragment.length() - 2);
    }

    public static void assertJsonPathFormat(String path)
    {
        Path compiledPath;
        try {
            compiledPath = PathCompiler.compile(path);
        }
        catch (InvalidPathException e) {
            throw new ConfigException(String.format("jsonpath %s, %s", path, e.getMessage()));
        }
        if (compiledPath.isFunctionPath()) {
            throw new ConfigException(String.format("Indefinite path and function path is not supported \"%s\"", path));
        }
    }
}