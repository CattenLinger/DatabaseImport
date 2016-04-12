package net.catten.db.importer.ne;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Catten on 2016/4/6.
 */
public class RawDataReader {
    private Properties readerProperties;

    private String splitChar;
    private int fieldCount = 0;
    private String sortOutPintFormat;

    public RawDataReader(String splitChar, String sqlExp) {
        readerProperties = new Properties();
        readerProperties.setProperty("reader.split",splitChar);
        readerProperties.setProperty("reader.sql",sqlExp);
        init();
    }

    public RawDataReader(Properties properties){
        readerProperties = properties;
        init();
    }

    private void init(){
        splitChar = readerProperties.getProperty("reader.split");
        for (byte c : readerProperties.getProperty("reader.sql").getBytes()) {
            if (c == '\\') continue;
            if (c == '?') fieldCount++;
        }
    }

    public int getFieldCount(){
        return fieldCount;
    }

    public String[] sortOut(String data){
        return data.split(splitChar);
    }

    public PreparedStatement sortOut(PreparedStatement preparedStatement,String data) throws SQLException {
        String[] sortResult = sortOut(data);
        for (int i = 0; i < sortResult.length; i++) {
            preparedStatement.setString(i + 1, sortResult[i]);
        }
        return preparedStatement;
    }

    public String getSqlExp() {
        return readerProperties.getProperty("reader.sql");
    }

    public String getSplitChar() {
        return readerProperties.getProperty("reader.split");
    }

    public String getSortOutPintFormat() {
        return sortOutPintFormat;
    }

    public void setSortOutPintFormat(String sortOutPintFormat) {
        this.sortOutPintFormat = sortOutPintFormat;
    }
}
