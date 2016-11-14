module hdb.serial;

import hdb.util;

struct Seriable(T, string idname = "id")
{
    alias inner = T;
      
    static if (!is(typeof(__traits(getMember, inner, idname))))
    {
        mixin(`long idname;`);
    }

    @dbInclude T t;
}