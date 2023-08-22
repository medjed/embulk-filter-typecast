package org.embulk.filter.typecast;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.internal.Path;
import com.jayway.jsonpath.internal.path.CompiledPath;
import com.jayway.jsonpath.internal.path.PathCompiler;
import com.jayway.jsonpath.internal.path.PathToken;
import org.embulk.config.ConfigException;

public class JsonPathUtil
{
    private JsonPathUtil() {}

    public static String getColumnName(String jsonPath)
    {
        CompiledPath compiledPath;
        try {
            compiledPath = (CompiledPath) PathCompiler.compile(jsonPath);
        }
        catch (InvalidPathException e) {
            throw new ConfigException(String.format("jsonpath %s, %s", jsonPath, e.getMessage()));
        }
        PathToken pathToken = compiledPath.getRoot();
        pathToken = pathToken.getNext(); // skip $
        return pathToken.getNext().toString();
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
        PathToken pathToken = compiledPath.getRoot();
        while (true) {
            assertSupportedPathToken(pathToken, path);
            if (pathToken.isLeaf()) {
                break;
            }
            pathToken = pathToken.next();
        }
    }

    protected static void assertSupportedPathToken(PathToken pathToken, String path)
    {
        if (pathToken instanceof ArrayPathToken) {
            ArrayIndexOperation arrayIndexOperation = ((ArrayPathToken) pathToken).getArrayIndexOperation();
            assertSupportedArrayPathToken(arrayIndexOperation, path);
        }
        else if (pathToken instanceof ScanPathToken) {
            throw new ConfigException(String.format("scan path token is not supported \"%s\"", path));
        }
        else if (pathToken instanceof FunctionPathToken) {
            throw new ConfigException(String.format("function path token is not supported \"%s\"", path));
        }
        else if (pathToken instanceof PredicatePathToken) {
            throw new ConfigException(String.format("predicate path token is not supported \"%s\"", path));
        }
    }

    protected static void assertSupportedArrayPathToken(ArrayIndexOperation arrayIndexOperation, String path)
    {
        if (arrayIndexOperation == null) {
            throw new ConfigException(String.format("Array Slice Operation is not supported \"%s\"", path));
        }
        else if (!arrayIndexOperation.isSingleIndexOperation()) {
            throw new ConfigException(String.format("Multi Array Indexes is not supported \"%s\"", path));
        }
    }
}
