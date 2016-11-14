module hdb.exception;

public import std.exception;
import std.conv : to;

class HDBException : Exception
{
    enum Type : uint
    {
        general,
        connectionFaild,
        noObjectFound
    }

    Type type;

    this(Type type, string msg, string file = __FILE__, size_t line = __LINE__,
         Throwable next = null) @nogc @safe pure nothrow
    {
        super(msg, file, line, next);
        this.type = type;
    }
}

auto dbEnforce(T)(T val, HDBException.Type type, string msg="",
        string file = __FILE__, size_t line = __LINE__) @safe pure
    if (is(typeof(!!val)))
{
    if (!!val) return val;
    if (msg.length == 0) msg = "HDBException." ~ type.to!string;
    throw new HDBException(type, msg, file, line);
}
