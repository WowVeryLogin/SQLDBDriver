#!/usr/bin/env dub
/+ dub.sdl:
    name "util-test"
    dependency "putil" path="../../../putil/"
    dependency "vibe-d-postgresql" version="~>0.2.19"
+/
///
module hdb.util;

import std.algorithm;
import std.string;
import std.range;
import std.traits;
import std.typecons;
import std.conv;
import std.meta;

public import vibe.data.bson;

public import putil.bson;
import putil.set;

///
alias DBAttrs = Set!string;

///
enum pKey = DBAttrs("PRIMARY KEY");
///
enum unique = DBAttrs("UNIQUE");
///
enum notNull = DBAttrs("NOT NULL");
///
enum asKey = DBAttrs("NOT NULL", "UNIQUE");
///
enum dbIgnore = DBAttrs("IGNORE");
///
enum dbInclude = DBAttrs("FLAT");

///
string getTableName(alias tn)()
{
    static if (is(typeof(tn) == string))
        return tn;
    else
        return tn.stringof.split(".")[$-1];
}

///
DBAttrs getDBAttrs(alias T, string name)()
{
    DBAttrs ret;
    foreach(dbattr; getUDAs!(__traits(getMember, T, name), DBAttrs))
        ret += dbattr;
    return ret;
}

///
struct FullFieldInfo
{
    private enum SEP = "@";

    string name;
    string typeName;
    DBAttrs attribs;

    const pure @property
    {
        string nameForDB() { return name.tr(SEP,"_"); }
        string nameForCode() { return name.tr(SEP,"."); }
    }

    auto addParentName(string pn)
    { name = pn ~ SEP ~ name; return this; }

    void setName(string n) { name = n; }

    string outDefPath;
}

///
FullFieldInfo[] getFlatNames(T, DBAttrs upperAttrs = DBAttrs.init)()
{
    FullFieldInfo[] ret;

    foreach (name; __traits(allMembers, T))
    {
        static if (is(typeof(__traits(getMember, T, name))))
        {
            enum propMix = "functionAttributes!(__traits(getMember, T, name))";
            alias ST = typeof(__traits(getMember, T, name));
            FullFieldInfo field;

            static if (!__traits(compiles, mixin(propMix)) && !(ST.stringof == "void"))
            {
                enum newAttrs = getDBAttrs!(T, name);
                static if (!(dbIgnore in newAttrs))
                {
                    enum curAttrs = upperAttrs + newAttrs;
                    static if (is(ST == struct) && !is(ST == Bson))
                    {
                        static if (dbInclude in newAttrs)
                        {
                            static if (curAttrs == dbInclude)
                                enum newCurAttrs = DBAttrs.init;
                            else
                                enum newCurAttrs = curAttrs - dbInclude;

                            foreach(nn; getFlatNames!(ST, newCurAttrs))
                                ret ~= nn;
                        }
                        else
                        {
                            foreach(nn; getFlatNames!(ST, curAttrs))
                                ret ~= nn.addParentName(name);
                        }
                    }
                    else
                    {
                        field.setName(name);
                        static if (is(ST == enum))
                        {
                            field.typeName = fullyQualifiedName!ST;//ST.stringof;
                            //if (fullyQualifiedName!ST.removechars(moduleName!ST));
                            field.outDefPath = moduleName!ST;
                        }
                        else
                            field.typeName = ST.stringof;
                        field.attribs = curAttrs;
                        ret ~= field;
                    }
                }
            }
        }
    }
    return ret;
}

///
struct Schema
{
    FullFieldInfo[string] data;

    this(FullFieldInfo[] data...)
    {
        foreach(field; data)
        {
            this.data[field.nameForDB] = field;
        }
    }

    static Schema forType(T)()
    {
        return Schema(getFlatNames!T);
    }

    string[] namesForDB() const @property
    {
        return this.data.keys;
    }
}

///
string switchSQLOp(string op)
{
    final switch (op)
    {
        case Selector.Op.lte : return "<=";
        case Selector.Op.gte : return "=>";
        case Selector.Op.gt : return ">";
        case Selector.Op.lt: return "<";
        case Selector.Op.eq: return "=";
        case Selector.Op.ne: return "!=";
    }
}

///
static string serializeSelectorToPostgresql(Bson selData)
{
    string[] res;
    string bKey;

    foreach(string key, Bson value; selData)
    {
        string[] selStr;
        if (key != "$or" && key != "$and" && key != "$not")
        {
            foreach(string op, Bson val; value)
            {
                if (val.type == Bson.Type.object)
                    selStr ~= key ~ switchSQLOp(op) ~ "'" ~ val.toString ~ "'";
                else
                    selStr ~= key ~ switchSQLOp(op) ~ val.toString.tr("\"","'");
            }
            res ~= selStr.join(" AND ");
        }
        else if (key == "$not")
        {
	    // https://docs.mongodb.com/manual/reference/operator/query/not/
            res ~= "NOT" ~ serializeSelectorToPostgresql(value);
        }
        else
        {
            foreach(Bson el; value)
                selStr ~= serializeSelectorToPostgresql(el);
            res ~= selStr.join(" " ~ key[1..$].toUpper ~ " ");
        }
    }

    return "(" ~ res.join(" AND ") ~ ")";
}


bool isSelector(T)() pure nothrow @nogc @safe { return is(T == Selector); }

///
struct Selector
{
    ///
    enum Op
    {
        eq = "$eq", /// equal
        lt = "$lt", /// less
        gt = "$gt", /// greater
        lte = "$lte", /// less or equal
        gte = "$gte", /// greater or equal
        ne = "$ne" /// not equal
    }

    ///
    enum Manips
    {
        or = "|",
        and = "&",
        not = "~"
    }

    ///
    static Selector eq(Args...)(Args args)
    { return Selector(args); }

    ///
    static Selector ne(Args...)(Args args)
    { return Selector(buildSelectorBson!"$ne"(args)); }

    ///
    static Selector lt(Args...)(Args args)
    { return Selector(buildSelectorBson!"$lt"(args)); }

    ///
    static Selector lte(Args...)(Args args)
    { return Selector(buildSelectorBson!"$lte"(args)); }

    ///
    static Selector gt(Args...)(Args args)
    { return Selector(buildSelectorBson!"$gt"(args)); }

    ///
    static Selector gte(Args...)(Args args)
    { return Selector(buildSelectorBson!"$gte"(args)); }

    static Selector list(T)(string name, T[] vals...)
    {
        auto sels = vals.map!(val => Bson([name: toBson(val)])).array;
        return Selector(Bson(["$or": Bson(sels)]));
    }

    ///
    Bson data;

    ///
    this(T)(T str)
    {
        enum _ctstr_ = format("this(%s);", getTypedArgs!(T,"str"));
        mixin(_ctstr_);
    }

    this(Bson value) { data = value; }

    this(Args...)(Args args)
    if (!anySatisfy!(isSelector, Args))
    { data = buildSelectorBson(args); }

    ///
    this(Selector[] selectors...)
    {
        this.data = Bson.emptyObject;

        Bson[] tmpArr;
        foreach(selector; selectors)
        {
            tmpArr ~= selector.data;
        }
        data["$"~Manips.and.to!string] = Bson(tmpArr);
    }

    //Выбор точного значения по данным из строки
    this(TableRow tr)
    {
        auto tmpBson = Bson.emptyObject;
        foreach (string key, Bson val; tr.data)
        {
            auto opBson = Bson.emptyObject;
            opBson[Op.eq] = val;
            tmpBson[key] = opBson;
        }
        data = tmpBson;
    }

    ///
    auto opUnary(string manip)() if (manip == Manips.not)
    {
        Selector ret;
        ret.data = Bson(["$not": data]);
        return ret;
    }

    ///
    auto opBinary(string manip)(Selector rhs)
    {
        enum mnp = cast(Manips)manip;
        Selector ret;
        ret.data = Bson(["$"~mnp.to!string: Bson([data, rhs.data])]);
        return ret;
    }

private:
    ///
    static auto getTypedArgs(T, string strName)()
    {
        enum fields = getFlatNames!T;
        string[] argStrings;
        foreach (field; fields)
        {
            argStrings ~= `"` ~ field.nameForDB ~ `", ` ~ strName ~ `.` ~ field.nameForCode;
        }
        return argStrings.join(", ");
    }

    ///
    static Bson buildSelectorBson(string opName = "$eq", Args...)(Args args)
            if (Args.length % 2 == 0)
    {
        auto ret = Bson.emptyObject;
        foreach (i, arg; args)
            static if (i % 2 == 0)
            {
                auto tmpBson = Bson.emptyObject;
                tmpBson[opName] = toBson(args[i + 1]);
                ret[args[i]] = tmpBson;
            }

        return ret;
    }
}

///
unittest
{
    struct C
    {
        bool xyz;
        @asKey double buble;
        Bson bson;
    }

    struct B
    {
        @notNull string val;
        C some;
    }

    struct A
    {
        @asKey @unique double x;
        @pKey int z;
        B b;
        @notNull C some;
    }

    Bson bs = Bson.emptyObject;
    bs["Field1"] = 2;
    bs["Field2"] = "AZAZA";
    auto a = A(3.14, 5, B("hello", C(true, 2.71, bs)), C(false, 1.23, bs));

    auto d = Selector.lt("x_1",2);
    auto d_str = serializeSelectorToPostgresql(d.data);
    assert(d_str == `(x_1<2)`);

    auto f = Selector("x_1",2);
    auto f_str = serializeSelectorToPostgresql(f.data);
    assert(f_str == `(x_1=2)`);

    auto g = Selector.gte("g_1","hey","g_2",3.1,"g_3",true);
    auto g_str = serializeSelectorToPostgresql(g.data);
    assert(g_str == `(g_1=>'hey' AND g_2=>3.1 AND g_3=>true)`);

    auto x = Selector(a);
    auto x_str = serializeSelectorToPostgresql(x.data);
    assert(x_str == (`(x=3.14 AND z=5 AND b_val='hello' AND b_some_xyz=true AND b_some_buble=2.71 AND`~
                     ` b_some_bson='{"Field2":"AZAZA","Field1":2}' AND some_xyz=false AND some_buble=1.23`~
                     ` AND some_bson='{"Field2":"AZAZA","Field1":2}')`));

    auto z = ~(~x & d) | g;
    auto z_str = serializeSelectorToPostgresql(z.data);
    assert(z_str == (`((NOT((NOT(x=3.14 AND z=5 AND b_val='hello' AND b_some_xyz=true AND b_some_buble=2.71`~
                     ` AND b_some_bson='{"Field2":"AZAZA","Field1":2}' AND some_xyz=false AND some_buble=1.23`~
                     ` AND some_bson='{"Field2":"AZAZA","Field1":2}')) AND (x_1<2))) OR (g_1=>'hey' AND g_2=>3.1 AND g_3=>true))`));
}

///
struct TableRow
{
    ///
    Bson[string] data;

    ///
    this(Bson[string] bsn)
    { data = Bson(bsn).get!(Bson[string]); }

    ///
    this(Bson bsn)
    { data = bsn.get!(Bson[string]); }

    ///
    this(Args...)(Args args)
    { data = buildBson(args).get!(Bson[string]); }

    ///
    void appendData(Args...)(Args args)
    { data = mergeBson(Bson(data), buildBson(args)).get!(Bson[string]); }

    alias data this;
}

///
TableRow serializeTableRow(string[] without=[], T)(T value)
{
    enum usedNames = getFlatNames!T.filter!(a=>!canFind(without, a.nameForDB));
    //pragma(msg, T, " ", usedNames.map!"a.name".array, " without: ", without);
    enum mix = usedNames.map!(a=> `"` ~ a.nameForDB ~ `"` ~ ", " ~ "value." ~ a.nameForCode).join(",");

    enum _ctstr_ = format(`return TableRow(%s);`, mix);
    static if (!is(typeof({ mixin(_ctstr_); }())))
        pragma(msg, __FILE__, ":", __LINE__+1, "\n", _ctstr_);
    mixin(_ctstr_);
}

///
T deserializeTableRow(T)(auto ref const(TableRow) tr) @property
{
    T res;

    enum fields = getFlatNames!T;

    static string mixString(FullFieldInfo[] a)
    {
        string ret;

        foreach (field; a)
        {
            if (field.outDefPath.length > 0)
            {
                 ret ~= "import " ~ field.outDefPath ~ ";\n";
            }
            ret ~= "res." ~ field.nameForCode ~ `= tr["` ~ field.nameForDB ~ `"]`;
            if (field.typeName != "Bson") ret ~= ".deserializeBson!(" ~ field.typeName ~ ");\n";
            else ret ~= ";\n";
        }

        return ret;
    }

    enum _ctstr_ = mixString(fields);
    static if (!is(typeof({ mixin(_ctstr_); }())))
        pragma(msg, __FILE__, ":", __LINE__+1, "\n", _ctstr_);
    mixin(_ctstr_);
    return res;
}

///
unittest
{
    struct C
    {
        bool xyz;
        @asKey double buble;
        Bson bson;
    }

    struct B
    {
        @notNull string val;
        C some;
    }

    struct A
    {
        @asKey @unique double x;
        @pKey int z;
        B b;
        @notNull C some;
    }

    Bson bs = Bson.emptyObject;
    bs["Field1"] = 2;
    bs["Field2"] = "AZAZA";
    auto a = A(3.14, 5, B("hello", C(true, 2.71, bs)), C(false, 1.23, bs));
    auto tblRow = serializeTableRow(a);
    assert(a == deserializeTableRow!(A)(tblRow));

    auto c = C(true, 2.71, bs);
    auto tblRowWithout = serializeTableRow!(["bson","xyz"])(c);
    assert(tblRowWithout.data == buildBson("buble",2.71).get!(Bson[string]));

    //assert(
    serializeSelectorToPostgresql(Selector(tblRow).data);
}

version(unittest) {
enum X
{
    ar = "ar_st",
    arr = "arr_st",
    arrr = "arrr_st",
}

struct C
{
    uint[3] xyz;
    enum T
    {
        testT = "test",
    }
    X en;
    T innerEnum;
    template Templ(R)
    {
        R tmp;
    }
    @asKey double buble;
    int prop() @property
    {
        return 1;
    }
    Bson bson;
}

unittest
{
    struct B
    {
        @notNull string val;
        C some;
    }

    struct A
    {
        @asKey @unique double x;
        @pKey int z;
        B b;
        @notNull C some;
    }

    Bson bs = Bson.emptyObject;
    bs["Field1"] = 2;
    bs["Field2"] = "AZAZA";
    auto a = A(3.14, 5, B("hello", C([1,2,3], X.arrr, C.T.testT, 2.71, bs)), C([4,5,6], X.ar, C.T.testT, 1.3, bs));
    auto tblRow = serializeTableRow(a);
    assert(a == deserializeTableRow!(A)(tblRow));
}
}

interface IHDBConnection
{
    bool isOpen() @property;
    void open();
    void close();
}

struct ScopeConn
{
    bool wasOpened;
    IHDBConnection conn;

    this(IHDBConnection conn)
    {
        this.conn = conn;

        wasOpened = conn.isOpen;
        if (!wasOpened)
            conn.open();
    }

    ~this()
    {
        if (!wasOpened)
            conn.close();
    }
}
