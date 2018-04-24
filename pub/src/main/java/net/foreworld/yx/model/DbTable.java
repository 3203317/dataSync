package net.foreworld.yx.model;

/**
 *
 * @author huangxin
 *
 */
public class DbTable {

	private String dbName;
	private String name;

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFullname() {
		return dbName + "." + name;
	}

}
