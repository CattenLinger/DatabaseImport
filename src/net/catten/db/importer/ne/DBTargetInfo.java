package net.catten.db.importer.ne;

/**
 * Created by Catten on 2016/4/6.
 */
public class DBTargetInfo {
    private String serverName;
    private String dbName;
    private String username;
    private String password;

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDBLink(){
        return String.format("jdbc:mysql://%s/%s",getServerName(),getDbName());
    }
}
