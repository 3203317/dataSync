package net.foreworld.yx.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.foreworld.yx.db.model.BinlogMasterStatus;
import net.foreworld.yx.db.model.DbTable;
import net.foreworld.yx.db.model.DbTableColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.code.or.binlog.impl.event.TableMapEvent;

/**
 *
 * @author huangxin
 *
 */
@Component
public final class DbUtil {

	private static Map<Long, DbTable> dbTableMap = new ConcurrentHashMap<>();
	private static Map<String, List<DbTableColumn>> dbTableColumnMap = new ConcurrentHashMap<>();

	private static final Logger logger = LoggerFactory.getLogger(DbUtil.class);

	/**
	 *
	 * @param tme
	 */
	public static void saveDbTableMap(TableMapEvent tme) {
		long tableId = tme.getTableId();

		DbTable d = dbTableMap.get(tableId);

		if (null == d) {
			d = new DbTable();
			dbTableMap.put(tableId, d);
		}

		d.setDbName(tme.getDatabaseName().toString());
		d.setName(tme.getTableName().toString());
	}

	/**
	 *
	 * @param tableId
	 * @return
	 */
	public static DbTable getDbTable(long tableId) {
		return dbTableMap.get(tableId);
	}

	/**
	 *
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static synchronized void reloaddbTableColumnMapMap()
			throws ClassNotFoundException, SQLException {
		dbTableColumnMap = getDbTableColumns();
	}

	/**
	 *
	 * @param fullname
	 * @return
	 */
	public static List<DbTableColumn> getDbTableColumns(String fullname) {
		return dbTableColumnMap.get(fullname);
	}

	private static String DRIVERCLASS;

	@Value("${jdbc.driverClass}")
	private void setDriverClass(String driverClass) {
		DRIVERCLASS = driverClass;
	}

	private static String HOST;

	@Value("${jdbc.url}")
	private void setHost(String host) {
		HOST = host;
	}

	private static String PORT;

	@Value("${jdbc.port}")
	private void setPort(String port) {
		PORT = port;
	}

	private static String USER;

	@Value("${jdbc.user}")
	private void setUser(String user) {
		USER = user;
	}

	private static String PASS;

	@Value("${jdbc.password}")
	private void setPass(String pass) {
		PASS = pass;
	}

	/**
	 *
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private static Connection getConn() throws ClassNotFoundException,
			SQLException {
		Class.forName(DRIVERCLASS);
		String url = "jdbc:mysql://" + HOST + ":" + PORT;
		return DriverManager.getConnection(url, USER, PASS);
	}

	/**
	 *
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private static Map<String, List<DbTableColumn>> getDbTableColumns()
			throws ClassNotFoundException, SQLException {
		Map<String, List<DbTableColumn>> dbTableColumnMap = new HashMap<String, List<DbTableColumn>>();

		DatabaseMetaData metaData = getConn().getMetaData();
		ResultSet _catalogs = metaData.getCatalogs();

		String tableType[] = { "TABLE" };

		while (_catalogs.next()) {
			String dbName = _catalogs.getString("TABLE_CAT");
			ResultSet _tables = metaData.getTables(dbName, null, null,
					tableType);

			while (_tables.next()) {
				String tableName = _tables.getString("TABLE_NAME");
				ResultSet _columns = metaData.getColumns(dbName, null,
						tableName, null);

				String fullname = dbName + "." + tableName;
				dbTableColumnMap.put(fullname, new ArrayList<DbTableColumn>());

				while (_columns.next()) {
					DbTableColumn dbTableColumn = new DbTableColumn();
					dbTableColumn.setName(_columns.getString("COLUMN_NAME"));
					dbTableColumn.setType(_columns.getString("TYPE_NAME"));
					dbTableColumnMap.get(fullname).add(dbTableColumn);
				}
			}
		}

		return dbTableColumnMap;
	}

	static String sql = "SHOW MASTER STATUS";

	public static BinlogMasterStatus getBinlogMasterStatus() {
		BinlogMasterStatus binlogMasterStatus = null;

		Connection conn = null;
		Statement stat = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			stat = conn.createStatement();
			rs = stat.executeQuery(sql);

			binlogMasterStatus = new BinlogMasterStatus();

			while (rs.next()) {
				binlogMasterStatus.setFile_name(rs.getString("File"));
				binlogMasterStatus.setPosition(rs.getLong("Position"));
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			binlogMasterStatus = null;
		} finally {

			if (null != rs) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}

			if (null != stat) {
				try {
					stat.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}

			if (null != conn) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}

		}

		return binlogMasterStatus;
	}

}
