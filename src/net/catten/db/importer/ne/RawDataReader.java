package net.catten.db.importer.ne;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Catten on 2016/4/6.
 */
public class RawDataReader {
    private String sqlExp;

    public int getFieldCount(){
        return 2;
    }

    public String[] sortOut(String data) throws ArrayIndexOutOfBoundsException {
        String[] buffer = data.split("----");
        System.out.printf("%s\t%s          ",buffer[0],buffer[1]);
        return buffer;
    }

    public String getSqlExp() {
        return sqlExp;
    }

    public void setSqlExp(String sqlExp) {
        this.sqlExp = sqlExp;
    }
}
