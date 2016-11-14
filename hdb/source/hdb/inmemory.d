module hdb.inmemory;

import std.algorithm;
import std.range;
import std.typecons;
import std.array;
import std.conv;

import std.exception;

import putil.bson;

import hdb.iface;
import hdb.exception;

import std.stdio;
void print()(auto ref const Bson val, string msg)
{
    stderr.writeln();
    stderr.writeln(msg);
    stderr.writeln(val.toJson.toPrettyString);
}

void print(Args...)(Args args)
{
    static if (__traits(compiles, stderr.writefln(args)))
        stderr.writefln(args);
    else
        stderr.writeln(args);
}

class InMDBTable : IHDBTable
{
protected:
    InMDB _db;
    string _name;
    Schema _schema;

    ref Bson[] rawTable() @property { return _db.rawTables[name]; }

    string idname;
    size_t idcounter=1;

    override void useAutoIncIdName(string name) { idname = name; }

    bool checkEq(const(Bson) bsn, const(Bson) row)
    {
        if (bsn.type == Bson.Type.object)
        {
            foreach (op, val; bsn.get!(Bson[string]))
            {
                enforce(op[0] == '$', "unsupported value op '" ~ op ~ "' (use: $gt, $gte, $lt, $lte)");
                if (!checkNumOp(op, val, row))
                    return false;
            }
        }
        else if (bsn.type == row.type)
        {
            return bsn == row;
        }
        else
        {
            auto a = bsn.toJson.toString;
            auto b = row.toJson.toString;
            return a == b;
        }
        return true;
    }

    bool checkNumOp(string op, const(Bson) val, const(Bson) rowval)
    {
        switch(op)
        {
            case "$gt":  return checkNumCtOp!">"(val, rowval);
            case "$gte": return checkNumCtOp!">="(val, rowval);
            case "$lt": return  checkNumCtOp!"<"(val, rowval);
            case "$lte": return checkNumCtOp!"<="(val, rowval);
            case "$ne" : return val != rowval;
            case "$eq": return val == rowval;
            default: assert(0, "unsuported value op '" ~ op ~ "'");
        }
    }

    static bool checkNumCtOp(string op)(const(Bson) a, const(Bson) b)
    {
        /+
            timestamp timestamp Q? структура с приватным полем, как сравнивать не понятно
            ++++++++++++++++++++++
            date      date
            ++++++++++++++++++++++
            double_   double_
            int_      int_
            long_     long_
        +/
        //enum TS = Bson.Type.timestamp;

        enum DT = Bson.Type.date;

        enum DBL = Bson.Type.double_;
        enum INT = Bson.Type.int_;
        enum LNG = Bson.Type.long_;

        static bool supported(Bson.Type tp)
        { return /+tp == TS ||+/ tp == DT || tp == DBL || tp == INT || tp == LNG; }

        enforce(supported(a.type), "unsupported type of value '" ~ a.type.to!string ~ "'");
        enforce(supported(b.type), "unsupported type of value '" ~ b.type.to!string ~ "'");

        static double getDouble(const(Bson) v)
        {
            if (v.type == DBL) return v.get!double;
            else if (v.type == INT) return v.get!int;
            else if (v.type == LNG) return v.get!long;
            else assert(0, "imposible!");
        }

        //if (a.type == TS && b.type == TS)
        //    mixin("return a.get!BsonTimestamp.to!long " ~ op ~ " b.get!BsonTimestamp.to!long;");
        //else
        if (a.type == DT && b.type == DT)
            mixin("return a.get!BsonDate.value " ~ op ~ " b.get!BsonDate.value;");
        else
        {
            double aa = getDouble(a), bb = getDouble(b);
            mixin("return aa " ~ op ~ " bb;");
        }
    }

    bool checkAnd(const(Bson[]) vals, const(Bson) row)
    {
        foreach (val; vals)
            if (!check(val, row))
                return false;
        return true;
    }

    bool checkOr(const(Bson[]) vals, const(Bson) row)
    {
        foreach (val; vals)
            if (check(val, row))
                return true;
        return false;
    }

    bool checkNot(const(Bson) val, const(Bson) row)
    { return !check(val, row); }

    bool checkLogicOp(string op, const(Bson) val, const(Bson) row)
    {
        switch(op)
        {
            case "$and": return checkAnd(val.get!(Bson[]), row);
            case "$or": return checkOr(val.get!(Bson[]), row);
            case "$not": return checkNot(val, row);
            default: assert(0, "unsupported logic op '" ~ op ~ "'");
        }
    }

    bool check(const(Bson) sel, const(Bson) row)
    {
        if (sel == Bson.init)
            return true;

        foreach (key, value; sel.get!(Bson[string]))
        {
            if (key[0] != '$')
            {
                if (!checkEq(value, row[key]))
                    return false;
            }
            else if (!checkLogicOp(key, value, row))
                return false;
        }

        return true;
    }

    void iterate(const(Selector) s, void delegate(size_t, ref Bson) func)
    {
        // тут значения копируются
        //rawTable.enumerate
        //    .filter!(r => check(s.data, r.value))
        //    .each!(r => func(r.index, r.value));

        foreach (i, ref val; rawTable)
        {
            if (!check(s.data, val)) continue;
            func(i, val);
        }
    }

public:

    this(InMDB db, string name, Schema schema)
    {
        _db = db;
        _name = name;
        _schema = schema;
    }

override:
    @property
    {
        const
        {
            string name() { return _name; }
            const(Schema) schema() { return _schema; }
        }

        IHDB db() { return _db; }
    }

    void insert(const(TableRow) values)
    {
        auto tmp = Bson(values.data);
        if (idname.length)
            tmp[idname] = Bson(idcounter++);

        rawTable ~= tmp;
    }

    void update(const(Selector) s, const(TableRow) nv)
    {
        iterate(s, (size_t i, ref Bson row)
        {
            foreach (key; nv.data.keys)
                row[key] = nv[key];
        });
    }

    void remove(const(Selector) s)
    {
        size_t[] rms;
        iterate(s, (size_t i, ref Bson row) { rms ~= i; });
        foreach (i; rms) rawTable = rawTable.remove(i);
    }

    TableRow[] getRows(const(Selector) s, const(Schema) returned)
    {
        TableRow[] ret;
        iterate(s, (size_t i, ref Bson row)
        {
            Bson buf = Bson.emptyObject;
            foreach (key; returned.namesForDB)
                buf[key] = row[key];
            ret ~= TableRow(buf);
        });
        return ret;
    }

    TableRow getOneRow(const(Selector) s, const(Schema) returned)
    {
        TableRow ret;
        size_t cnt;
        iterate(s, (size_t i, ref Bson row)
        {
            cnt++;
            ret.data = row.get!(Bson[string])
                        .byKeyValue
                        .filter!(p => returned.namesForDB.canFind(p.key))
                        .map!(p => tuple(p.key, p.value))
                        .assocArray;
        });

        dbEnforce(cnt > 0, HDBException.Type.noObjectFound);
        return ret;
    }
}

class InMDBConnection : IHDBConnection
{
    int opened;
    invariant { assert(opened == 1 || opened == 0); }
    bool isOpen() @property { return !!opened; }
    void open() { opened++; }
    void close() { opened--; }
}

class InMDB : IHDB
{
package:

protected:
    InMDBConnection _conn;

public:
    Bson[][string] rawTables;
    this() { _conn = new InMDBConnection; }

override:
    IHDBTable createTable(string name, Schema schema)
    {
        rawTables[name] = Bson[].init;
        return new InMDBTable(this, name, schema);
    }

    bool checkTable(string name)
    {
        return cast(bool)(name in rawTables);
    }

    IHDBTable createOrGetTable(string name, Schema schema)
    {
        if (name !in rawTables)
            rawTables[name] = Bson[].init;

        return new InMDBTable(this, name, schema);
    }

    IHDBConnection conn() @property { return _conn; }
    void deleteTable(string name) { rawTables.remove(name); }

    void dropBase()
    {
        rawTables.clear();
    }
}

unittest
{
    import putil.set;
    static struct Point { long id; int x, y; }
    alias PSet = Set!Point;
    auto db = new InMDB;

    auto points = db.createTypedTable!(Point, "id")("points");
    auto a = Point(0, 3, 5);
    auto b = Point(2, 5, 3);
    auto c = Point(1, 2, 7);
    auto p1id = points.insert(a);
    auto p2id = points.insert(b);
    auto p3id = points.insert(c);

    assert(points.getOne(p1id).x == a.x);
    assert(points.getOne(p1id).y == a.y);
    assert(points.getOne(p2id).x == b.x);
    assert(points.getOne(p2id).y == b.y);

    a = points.getOne(p1id);
    b = points.getOne(p2id);
    c = points.getOne(p3id);

    assert(PSet(points.get([p1id, p2id])) == PSet(a,b));
    assert(PSet(points.get([p1id, p3id])) == PSet(a,c));
    assert(PSet(points.get([p2id, p3id])) == PSet(b,c));
    assert(points.getOne(Selector("x", 3)) == a);

    c.x = 12;
    points.update(c);
    assert(points.getOne(Selector("x", 12)) == c);

    points.update(p1id, TableRow("y", 10));
    assert(points.getOne(p1id) != a);
    assert(points.getOne(p1id) == Point(p1id, 3, 10));
    a = points.getOne(a.id);
    assert(points.getOne(p1id) == a);

    assert(PSet(points.get()) == PSet(b,c,a));
    points.remove(b.id);
    assert(PSet(points.get()) == PSet(c,a));
}
