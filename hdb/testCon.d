#!/usr/bin/env dub
/+ dub.sdl:
    name "util-test"
    dependency "vibe-d-postgresql" version="~>0.2.19"
+/
import vibe.db.postgresql;
void main()
{
    auto post_client = new shared PostgresClient("dbname="~"postgres"~" user=User359", 4);
    auto post_conn = post_client.lockConnection();
    post_conn.execStatement("DROP DATABASE IF EXISTS test;");
    post_conn.execStatement("CREATE DATABASE test;");
    auto client = new shared PostgresClient("dbname="~"test"~" user=User359", 4);
    auto conn = client.lockConnection();
    conn.execStatement("CREATE TABLE testTable (a real);");
    delete conn;
    conn.dropConnection();
    
    post_conn.execStatement("DROP DATABASE IF EXISTS test;");
}