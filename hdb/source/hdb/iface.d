module hdb.iface;

import std.range;
import std.algorithm;

//import hdb.serial;
public import hdb.util;

struct Seriable(T, string idname)
{
    mixin("long " ~ idname ~ ";");
    alias value this;

    @dbInclude
    T value;
}

/++ Не типизированная таблица
    Каждый метод обращения к базе открывает соединение через ScopeConn
 +/
interface IHDBTable
{
protected:
    void useAutoIncIdName(string name);

public:
    @property
    {
        const
        {
            string name();
            const(Schema) schema();
        }

        IHDB db();
    }

    void insert(const(TableRow) values);
    void update(const(Selector) s, const(TableRow) new_values);
    void remove(const(Selector) s);

    TableRow[] getRows(const(Selector) s, const(Schema) returned);
    TableRow getOneRow(const(Selector) s, const(Schema) returned);
}

class TypedHDBTable(T, string idname="id") : IHDBTable
{
    protected override void useAutoIncIdName(string name) { base.useAutoIncIdName(name); }
    static assert(idname.length == 0 || isIdentifier!idname, "invalid idname '" ~ idname ~ "'");

    IHDBTable base;
    bool autogenid;

    this(IHDBTable base, bool autogenid=true)
    {
        this.base = base;
        this.autogenid = autogenid;
        static if (idname.length)
            useAutoIncIdName(idname);
    }

    static if (is(typeof(mixin("T.init."~idname)) ID))
    {
        ID insert(const(T) value)
        {
            auto _ = db.scopeConn;
            auto tr = serializeTableRow!([idname])(value);
            base.insert(tr);
            return base.getOneRow(Selector(tr), Schema(FullFieldInfo(idname,"int")))[idname].get!ID;
        }

        // только если в структуре есть id не нужен селектор
        void update(const(T) value)
        { base.update(Selector(idname, value.id), serializeTableRow!([idname])(value)); }

        void update(ID id, const(T) value)
        { base.update(Selector(idname, id), serializeTableRow!([idname])(value)); }

        void update(ID id, const(TableRow) new_values)
        { base.update(Selector(idname, id), new_values); }

        void remove(ID id) { base.remove(Selector(idname, id)); }

        T getOne(ID id) { return getOne(Selector(idname, id)); }

        T[] get(ID[] ids)
        {
            Bson[] buf;
            foreach (id; ids)
                buf ~= Bson([idname: Bson(id)]);

            Selector sel;
            sel.data = Bson(["$or": Bson(buf)]);
            return get(sel);
        }
    }
    else
        void insert(const(T) value) { base.insert(serializeTableRow(value)); }

    void update(const(Selector) s, const(T) value)
    { base.update(s, serializeTableRow(value)); }

    void remove(const(T) value)
    { base.remove(Selector(serializeTableRow(value))); }

    T[] get(const(Selector) s=Selector.init)
    {
        return base.getRows(s, Schema.forType!T).map!(a => a.deserializeTableRow!T).array;
    }

    T getOne(const(Selector) s)
    {
        return base.getOneRow(s, Schema.forType!T).deserializeTableRow!T;
    }

override:

    @property
    {
        const
        {
            string name() { return base.name; }
            const(Schema) schema() { return base.schema; }
        }

        IHDB db() { return base.db; }
    }

    void insert(const(TableRow) row) { base.insert(row); }
    void update(const(Selector) s, const(TableRow) row) { base.update(s, row); }
    void remove(const(Selector) s) { base.remove(s); }

    TableRow[] getRows(const(Selector) s, const(Schema) returned)
    { return base.getRows(s, returned); }

    TableRow getOneRow(const(Selector) s, const(Schema) returned)
    { return base.getOneRow(s, returned); }
}

interface IHDB
{
    IHDBTable createTable(string name, Schema schema);
    IHDBTable createOrGetTable(string name, Schema schema);
    bool checkTable(string name);

    IHDBConnection conn() @property;

    void deleteTable(string name);

    void dropBase();
    static void dropBase(string name);
    static bool checkDB(string name);

    final ScopeConn scopeConn() @property
    { return ScopeConn(this.conn); }

    final auto createTypedTable(T, string idname="id", bool autogenid=true)(string name)
    { return new TypedHDBTable!(T, idname)(createTable(name, Schema.forType!(Seriable!(T,idname))), autogenid); }

    final auto createOrGetTypedTable(T, string idname="id", bool autogenid=true)(string name)
    { return new TypedHDBTable!(T, idname)(createOrGetTable(name, Schema.forType!(Seriable!(T,idname))), autogenid); }
}

private bool isIdentifier(string val)()
{
    static auto idIdentifierImpl(string v)()
    { mixin("long " ~ v ~ "; return " ~ v ~ ";"); }
    return __traits(compiles, idIdentifierImpl!val);
}
