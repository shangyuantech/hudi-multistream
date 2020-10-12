package org.apache.hudi.multistream.common;

public class HudiTable {

    private String db;
    private String table;
    private String path;

    public HudiTable(String db, String table) {
        this.db = db;
        this.table = table;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "HudiTable{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", path='" + path + '\'' +
                '}';
    }
}

