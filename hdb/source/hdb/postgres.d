#!/usr/bin/env dub
/+ dub.sdl:
    name "test"
    dependency "vibe-d-postgresql" version="~>0.2.19"
+/
//module qreport.db.postges;
module hdb.postgres;

version(postgres):

import std.algorithm;
import std.string;
import vibe.db.postgresql;
import hdb.iface;
import hdb.util;
import hdb.exception;
import std.stdio;

 ///
class DBTable : IHDBTable
{
private:
    string tb_name;
    IHDB tb_db;
    Schema tb_schema;
    Bson parseType(string typeName, const(Value) addType)
    {
        //if (typeName.canFind("ubyte"))
        //{
          //  return Bson(addType.as!PGbytea);
        //}
        //else
       // {
            switch (typeName)
            {
                //case "serial" : return Bson(addType.as!PGs)
                case "double": return Bson(addType.as!PGdouble_precision);
                case "float" : return Bson(addType.as!PGreal);
                case "string": return Bson(addType.as!PGtext);
                case "bool" : return Bson(addType.as!PGboolean);
                case "int" : return Bson(addType.as!PGinteger);
                case "long" : return Bson(addType.as!PGbigint);
                case "Bson" : return Bson(addType.as!PGjson);//parseJsonString(addType.as!PGtext));
                default: return Bson(addType.as!PGtext);
            }
       // }
    }

public:
    ///
    this(string name, Schema schema, IHDB db)
    {
        this.tb_name = name;
        this.tb_db = db;
        this.tb_schema = schema;
    }

override: 
    ///
    protected void useAutoIncIdName(string name) 
    {
        writeln(tb_schema);
        writeln(name);
        this.tb_schema.data[name].typeName = "serial";
        db.deleteTable(this.tb_name);
        db.createTable(this.tb_name, this.tb_schema);
    }

    @property
    {
        const
        {
            ///
            string name()
            {
                return tb_name;
            }

            ///
            const(Schema) schema()
            {
                return tb_schema;
            }
        }

        ///
        IHDB db()
        {
            return tb_db;
        }
    }

    ///
    void insert(const(TableRow) values)
    {
        auto _ = db.scopeConn;
        string query;
        query ~= "INSERT INTO " ~ this.name ~ " (";
        
        string[] valQuery;
        string[] nameQuery;
        
        foreach(string key, Bson value; values.data)
        {
            nameQuery ~= key;
        }

        foreach (string key, Bson value; values.data)
        {
            if (schema.data[key].typeName == "Bson")
                valQuery ~= "'"~values.data[key].toString ~ "'";
            else
                valQuery ~= values.data[key].toString.tr("\"","'");
        }
        /*foreach (key; this.schema.data)
        {
            if (key.typeName == "Bson")
                valQuery ~= "'"~values.data[key.nameForDB].toString ~ "'";
            else
                valQuery ~= values.data[key.nameForDB].toString.tr("\"","'");
        }*/

        (cast(PConnection)tb_db.conn).exec(query~nameQuery.join(", ")~") VALUES ("~valQuery.join(", ") ~ ");");
    }

    ///
    void update(const(Selector) s, const(TableRow) new_values)
    {
        auto _ = db.scopeConn;
        string query;
        query ~= "UPDATE " ~ this.name ~ " SET ";

        string[] tmpFlds;
        foreach (string key, Bson value; new_values.data)
        {
            auto tmpFld = key ~ " = ";
            if (value.type == Bson.Type.object)
                tmpFld ~= "'" ~ value.toString ~ "'";
            else
                tmpFld ~= value.toString.tr("\"","'");
            tmpFlds ~= tmpFld;
        }

        query ~= tmpFlds.join(", ") ~ " WHERE " ~ serializeSelectorToPostgresql(s.data);

        (cast(PConnection)tb_db.conn).exec(query ~ ";");
    }

    ///
    void remove(const(Selector) s)
    {
        auto _ = db.scopeConn;
        string query = "DELETE FROM " ~ this.name ~ " WHERE ";
        query ~= serializeSelectorToPostgresql(s.data);
        (cast(PConnection)tb_db.conn).exec(query ~ ";");
    }

    TableRow[] getRows(const(Selector) s, const(Schema) returned)
    {
        auto _ = db.scopeConn;

        string query = "SELECT ";
        if (returned.data.length > 0)
        {
            string[] tmpQr;
            foreach (field; returned.data)
            {
                if (field.typeName == "Bson")
                {
                    tmpQr ~= field.nameForDB ~ "::json";
                }
                else
                {
                    tmpQr ~= field.nameForDB;
                }
            }
            query ~= tmpQr.join(", ");
        }
        else
            query ~= "* ";

        query ~= " FROM " ~ tb_name;

        if (s.data != Bson.emptyObject)
        {
            query ~= " WHERE ";
            query ~= serializeSelectorToPostgresql(s.data);
        }

        auto result = (cast(PConnection)tb_db.conn).exec(query ~ ";");

        TableRow[] res;
        foreach (row; rangify(result))
        {
            TableRow tmp;
            foreach (field; returned.data)
            {
                tmp.appendData(field.nameForDB, parseType(field.typeName,row[field.nameForDB]));
            }
            res ~= tmp;
        }
        return res;
    }

    TableRow getOneRow(const(Selector) s, const(Schema) returned)
    {
        auto _ = db.scopeConn;

        string query = "SELECT ";
        if (returned.data.length > 0)
        {
            string[] tmpQr;
            foreach (field; returned.data)
            {
                if (field.typeName == "Bson")
                {
                    tmpQr ~= field.nameForDB ~ "::json";
                }
                else
                {
                    tmpQr ~= field.nameForDB;
                }
            }
            query ~= tmpQr.join(", ");
        }
        else
            query ~= "* ";

        query ~= " FROM " ~ tb_name;

        if (s.data != Bson.emptyObject)
        {
            query ~= " WHERE ";
            query ~= serializeSelectorToPostgresql(s.data);
        }

        auto result = (cast(PConnection)tb_db.conn).exec(query ~ ";");

        TableRow res;
        foreach (field; returned.data)
        {
            res.appendData(field.nameForDB, parseType(field.typeName,result[0][field.nameForDB]));
        }

        return res;
    }
}

///
class PConnection : IHDBConnection
{
private:
    size_t counter;
    string name;
    shared PostgresClient client;
    string sqlDataBuff;
    LockedConnection!(__Conn) conn;
    bool con_open;

public:
import std.stdio;
    ///
    this(string dbName = "")
    {
        counter = 0;

        if (dbName.length > 0)
            name = dbName;
        else
            name = "postgres";

        try {
            client = new shared PostgresClient("dbname="~name~" user=User359", 4);
        }
        catch (ConnectionException)
        {
            throw new HDBException(HDBException.Type.connectionFaild, "Не удалось подключиться к бд");
        }
        sqlDataBuff = "BEGIN; ";
        con_open = false;
    }
import std.stdio;
    void push(string query)
    {
        counter++;
        sqlDataBuff ~= query ~ " ";
    }

    immutable(Answer) exec(string query = "")
    {
        scope(exit){stderr.writeln("Execution finished;");}

        QueryParams p;

       // try {
            if (counter > 0)
            {
                sqlDataBuff ~= "COMMIT;";
                p.sqlCommand = sqlDataBuff;
                stderr.writeln("Try to exec transaction: " ~ sqlDataBuff);
            
                auto buffRes = conn.execStatement(p);
                stderr.writeln("Transaction execution finished");
                sqlDataBuff = "BEGIN; ";
                counter = 0;
                if (query.length > 0)
                {
                    p.sqlCommand = query;
                    stderr.writeln("Try to exec: " ~ query);
                    return conn.execStatement(p);
                }
                else
                {
                    return buffRes;
                }
            }
            else
            {
                p.sqlCommand = query;
                stderr.writeln("Try to exec: " ~ query);
                return conn.execStatement(p);
            }
        //}
       /+ catch(AnswerCreationException)
        {
            throw new HDBException(HDBException.Type.noObjectFound, "Запрос не нашел объектов в бд");
        }
        catch(ConnectionException)
        {
            throw new HDBException(HDBException.Type.connectionFaild, "Соединение с бд нарушено");
        }
        catch(Exception)
        {
            throw new HDBException(HDBException.Type.general, "Что-то пошло не так");
        }+/
    }

override:
    bool isOpen() @property
    {
        return con_open;
    }

    void open()
    {
        stderr.writeln("Open connection");
        con_open = true;
       // try {
            conn = client.lockConnection();
       // }
        //catch(ConnectionException)
       // {
            //throw new HDBException(HDBException.Type.connectionFaild, "Соединение с бд нарушено");
       // }
    }

    void close()
    {
        stderr.writeln("Close connection");
        if (counter > 0) { exec(); counter = 0; }
        con_open = false;
        delete conn;
    }
}

class PostgresDB : IHDB
{
private:
    string name;
    PConnection con;
    static string convertType(string type)
        {
            switch (type) {
                case "bool" : return "boolean";
                case "int" : return "integer";
                case "string" : return "text";
                case "double" : return "double precision";
                case "float" : return "real";
                case "long" : return "bigint";
                case "Bson" : return "jsonb";
                default: return type;
            }
        }
    
    void createDB(string name)
    {
        auto tmpCon = new PConnection();
        auto _ = ScopeConn(tmpCon);
        tmpCon.exec("CREATE DATABASE " ~ name ~";");
    }

public:
    this (string name)
    {
        this.name = name.toLower;
        if (!checkDB(name.toLower))
            this.createDB(name.toLower);
        this.con = new PConnection(name.toLower);    
    }

    static void dropBase(string name)
    {
        auto tmpCon = new PConnection();
        auto _ = ScopeConn(tmpCon);
        tmpCon.exec("DROP DATABASE IF EXISTS "~name~";");
    }

    static bool checkDB(string name)
    {
        auto tmpCon = new PConnection();
        auto _ = ScopeConn(tmpCon);
        auto res = tmpCon.exec("SELECT 1 FROM pg_database WHERE datname ='"~ name.toLower ~"';");
        if (res.length > 0)
            return true;
        else
            return false;
    }

override:
    IHDBConnection conn() @property
    {
        return con;
    }

    void deleteTable(string name)
    {
        auto _ = scopeConn;
        if (checkTable(name))
           (cast(PConnection)conn).exec("DROP TABLE " ~ name ~ ";");
    }


    void dropBase()
    {
        auto _ = scopeConn;
        (cast(PConnection)conn).exec("DROP SCHEMA public CASCADE;");
        (cast(PConnection)conn).exec("CREATE SCHEMA public;");
        (cast(PConnection)conn).exec("GRANT ALL ON SCHEMA public TO postgres;");
        (cast(PConnection)conn).exec("GRANT ALL ON SCHEMA public TO public;");
    }

   /* void dropBase()
    {
        throw new HDBException(HDBException.Type.connectionFaild, "Невозможно сбросить подключенную базу");
    }*/

    bool checkTable(string name)
    {
        auto _ = scopeConn;
        auto res = (cast(PConnection)conn).exec("SELECT * FROM pg_tables WHERE tablename='"~name.toLower~"';");
        if (res.length > 0)
            return true;
        else
            return false;
    }

    IHDBTable createTable(string name, Schema schema)
    {
        if (!checkTable(name))
        {
            return createOrGetTable(name, schema);
        }
        else
        {
            deleteTable(name);
            return createOrGetTable(name, schema);
        }
    }

    IHDBTable createOrGetTable(string name, Schema schema)
    {
        if (!checkTable(name))
        {
            auto _ = scopeConn;
            string[] createTableQuery;
            auto lines = schema.data;

            foreach (line; lines)
            {
                auto type = convertType(line.typeName);
                createTableQuery ~= [line.nameForDB, type, line.attribs.values.join(" ")].join(" ");
            }

            auto query = "CREATE TABLE " ~ name ~"(" ~ createTableQuery.join(",\n") ~ ");";

            (cast(PConnection)conn).exec(query);
        }
        return new DBTable(name, schema, this);
        //IF NOT EXISTS
    }
}


unittest
{
    static struct C
    {
        @dbIgnore bool xyz;
        Bson bs;
        @asKey:
        int buble;
        bool opEquals()(auto ref const C rhs) const
        {
            import std.conv;
            return (rhs.to!string == this.to!string);
        }
    }

    struct B
    {
        @notNull string val;
        C some;
    }

    struct A
    {
        int id;
        @asKey @unique int x;
        @pKey int z;
        B b;
        @notNull C some;
        bool opEquals()(auto ref const A rhs) const
        {
            return (rhs.x == this.x && rhs.z == this.z && rhs.b == this.b && rhs.some == this.some);
        }
    }

    void testDB()//TypedHDBTable!A testDB()
    {
        PostgresDB.dropBase("test");
        assert(!PostgresDB.checkDB("test"));
        auto myDB = new PostgresDB("test");
        assert(PostgresDB.checkDB("test"));

        auto testDB = new PostgresDB("test");
        auto table = testDB.createTable("testTable", Schema.forType!A);
                
        auto typedTable = new TypedHDBTable!A(table);
        assert(testDB.checkTable("testTable") == true);
        testDB.deleteTable("testTable");
        testDB.deleteTable("testTable");
        assert(testDB.checkTable("testTable") == false);

        auto testTable = testDB.createTable("testTable", Schema.forType!A);
        auto typedTestTable = new TypedHDBTable!(A)(table);
        auto forGetTable = testDB.createOrGetTable("testTable", Schema.forType!A);
        //return new TypedHDBTable!A(forGetTable);
    }

    void testTables()//TypedHDBTable!A typedTable)
    {
        Bson bs = Bson.emptyObject;
        bs["testNumField"] = 1;
        bs["testTextField"] = "World!";
        auto a = A(0, 3, 5, B("hello", C(true, bs, 2)), C(false, bs, 1));

        auto orTbl = new PostgresDB("test").createOrGetTable("testTable", Schema.forType!A);
        auto typedTable = new TypedHDBTable!(A)(orTbl);
        typedTable.insert(a);
        auto sel = Selector("x",3);

        auto b = typedTable.getOne(sel);
        b.b.some.xyz = true;

        import std.stdio;
        writeln(b);
        writeln(a);
        assert(a == b);

        auto c = A(0, 3, 6, B("bye", C(false, bs, 3)), C(false, bs, 2));
        auto res1 = typedTable.get(sel);
        foreach(el; res1)
        {
            el.b.some.xyz = true;
            assert(el == a || el == b || el == c);
        }

        auto d = A(0, 4, 1, B("upTest", C(false, bs, 10)), C(false, bs, 5));
        typedTable.update(Selector(a),d);
        auto res2 = typedTable.getOne(Selector("x",4));
        assert(res2 == d);
        assertThrown(typedTable.getOne(Selector(a)));

        typedTable.remove(b);
        typedTable.remove(Selector(b));
        assertThrown(typedTable.getOne(Selector(b)));
    }

    testDB();
    testTables();
}