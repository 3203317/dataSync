package net.foreworld.yx.listener;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.foreworld.yx.db.DbUtil;
import net.foreworld.yx.db.MonitorDbUtil;
import net.foreworld.yx.db.model.DbTable;
import net.foreworld.yx.db.model.DbTableColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

/**
 *
 * @author huangxin
 *
 */
@Component
public class MyBinlogEventListener implements BinlogEventListener {

	private Gson gson = new GsonBuilder().serializeNulls().create();

	@Value("${monitor.db.table}")
	private String monitor_db_table;

	private static final Logger logger = LoggerFactory
			.getLogger(MyBinlogEventListener.class);

	@Override
	public void onEvents(BinlogEventV4 be) {
		if (null == be) {
			logger.error("binlog event is null");
			return;
		}

		int eventType = be.getHeader().getEventType();

		switch (eventType) {
		case MySQLConstants.TABLE_MAP_EVENT: {
			logger.info("TABLE_MAP_EVENT: {}", MySQLConstants.TABLE_MAP_EVENT);
			DbUtil.saveDbTableMap((TableMapEvent) be);
			break;
		}

		case MySQLConstants.UPDATE_ROWS_EVENT_V2: {
			logger.info("UPDATE_ROWS_EVENT_V2: {}",
					MySQLConstants.UPDATE_ROWS_EVENT_V2);
			try {
				UPDATE_ROWS_EVENT_V2((UpdateRowsEventV2) be);
			} catch (ClassNotFoundException | SQLException e) {
				logger.error(e.getMessage(), e);
			}
			break;
		}

		case MySQLConstants.WRITE_ROWS_EVENT_V2: {
			logger.info("WRITE_ROWS_EVENT_V2: {}",
					MySQLConstants.WRITE_ROWS_EVENT_V2);
			try {
				WRITE_ROWS_EVENT_V2((WriteRowsEventV2) be);
			} catch (ClassNotFoundException | SQLException e) {
				logger.error(e.getMessage(), e);
			}
			break;
		}

		case MySQLConstants.DELETE_ROWS_EVENT_V2: {
			logger.info("DELETE_ROWS_EVENT_V2: {}",
					MySQLConstants.DELETE_ROWS_EVENT_V2);
			try {
				DELETE_ROWS_EVENT_V2((DeleteRowsEventV2) be);
			} catch (ClassNotFoundException | SQLException e) {
				logger.error(e.getMessage(), e);
			}
			break;
		}

		 case MySQLConstants.XID_EVENT: {
		 logger.info("XID_EVENT: {}", MySQLConstants.XID_EVENT);
		 XidEvent xe = (XidEvent) be;
		 logger.info("xid event xid: {}", xe.getXid());
		 break;
		 }

		case MySQLConstants.QUERY_EVENT: {
			logger.info("QUERY_EVENT: {}", MySQLConstants.QUERY_EVENT);
			try {
				QUERY_EVENT((QueryEvent) be);
			} catch (ClassNotFoundException | SQLException e) {
				logger.error(e.getMessage(), e);
			}
			break;
		}

		default: {
			logger.debug("default event type: {}", eventType);
			break;
		}
		}
	}

	/**
	 *
	 * @param qe
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void QUERY_EVENT(QueryEvent qe) throws ClassNotFoundException,
			SQLException {

		MonitorDbUtil.setCfgBinlog();

		if ("BEGIN".equals(qe.getSql().toString())) {
			DbUtil.reloaddbTableColumnMapMap();
			return;
		}

		String topic_name = checkExist(qe.getDatabaseName().toString(), "*",
				null);

		if (null == topic_name)
			return;

		// if (!checkExist(qe.getDatabaseName().toString(), ""))
		// return;

		JsonObject jo = new JsonObject();

		jo.addProperty("db_name", qe.getDatabaseName().toString());
		jo.addProperty("opt_time", qe.getHeader().getTimestampOfReceipt());
		jo.addProperty("sql", qe.getSql().toString());

		// 1.0.1 -->
		// MonitorDbUtil.setSql(jo.toString(), topic_name);
		// <-- 1.0.1
	}

	/**
	 *
	 * @param wrev
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private void WRITE_ROWS_EVENT_V2(WriteRowsEventV2 wrev)
			throws ClassNotFoundException, SQLException {

		DbTable dbTable = DbUtil.getDbTable(wrev.getTableId());

		String topic_name = checkExist(dbTable.getDbName(), dbTable.getName(),
				null);

		if (null == topic_name)
			return;

		// if (!checkExist(dbTable.getDbName(), dbTable.getName()))
		// return;

		JsonObject jo = new JsonObject();

		jo.addProperty("db_name", dbTable.getDbName());
		jo.addProperty("table_name", dbTable.getName());
		jo.addProperty("opt", "insert");
		jo.addProperty("opt_time", wrev.getHeader().getTimestampOfReceipt());

		List<Row> lr = wrev.getRows();

		for (int i = 0; i < lr.size(); i++) {
			List<Column> lc = lr.get(i).getColumns();

			Map<String, String> afterMap = getDbTableColumnMap(lc,
					dbTable.getDbName(), dbTable.getName());

			if (null != afterMap && 0 < afterMap.size()) {
				jo.addProperty("after", gson.toJson(afterMap));
				MonitorDbUtil.setSql(jo.toString(), topic_name);
			}
		}

		MonitorDbUtil.setCfgBinlog();
	}

	/**
	 *
	 * @param drev
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void DELETE_ROWS_EVENT_V2(DeleteRowsEventV2 drev)
			throws ClassNotFoundException, SQLException {
		DbTable dbTable = DbUtil.getDbTable(drev.getTableId());

		String topic_name = checkExist(dbTable.getDbName(), dbTable.getName(),
				null);

		if (null == topic_name)
			return;

		// if (!checkExist(dbTable.getDbName(), dbTable.getName()))
		// return;

		JsonObject jo = new JsonObject();

		jo.addProperty("db_name", dbTable.getDbName());
		jo.addProperty("table_name", dbTable.getName());
		jo.addProperty("opt", "delete");
		jo.addProperty("opt_time", drev.getHeader().getTimestampOfReceipt());

		List<Row> lr = drev.getRows();

		for (int i = 0; i < lr.size(); i++) {
			List<Column> lc = lr.get(i).getColumns();

			Map<String, String> beforeMap = getDbTableColumnMap(lc,
					dbTable.getDbName(), dbTable.getName());

			if (null != beforeMap && 0 < beforeMap.size()) {
				jo.addProperty("before", gson.toJson(beforeMap));
				MonitorDbUtil.setSql(jo.toString(), topic_name);
			}
		}

		MonitorDbUtil.setCfgBinlog();
	}

	/**
	 *
	 * @param urev
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private void UPDATE_ROWS_EVENT_V2(UpdateRowsEventV2 urev)
			throws ClassNotFoundException, SQLException {
		DbTable dbTable = DbUtil.getDbTable(urev.getTableId());

		String topic_name = checkExist(dbTable.getDbName(), dbTable.getName(),
				null);

		if (null == topic_name)
			return;

		// if (!checkExist(dbTable.getDbName(), dbTable.getName()))
		// return;

		JsonObject jo = new JsonObject();

		jo.addProperty("db_name", dbTable.getDbName());
		jo.addProperty("table_name", dbTable.getName());
		jo.addProperty("opt", "update");
		jo.addProperty("opt_time", urev.getHeader().getTimestampOfReceipt());

		List<Pair<Row>> lpr = urev.getRows();

		for (int i = 0; i < lpr.size(); i++) {
			Pair<Row> pr = lpr.get(i);
			List<Column> rbc = pr.getBefore().getColumns();
			List<Column> rac = pr.getAfter().getColumns();

			Map<String, String> beforeMap = getDbTableColumnMap(rbc,
					dbTable.getDbName(), dbTable.getName());

			Map<String, String> afterMap = getDbTableColumnMap(rac,
					dbTable.getDbName(), dbTable.getName());

			if (null != beforeMap && null != afterMap && 0 < beforeMap.size()
					&& 0 < afterMap.size()) {
				jo.addProperty("before", gson.toJson(beforeMap));
				jo.addProperty("after", gson.toJson(afterMap));
				MonitorDbUtil.setSql(jo.toString(), topic_name);
			}
		}

		MonitorDbUtil.setCfgBinlog();
	}

	private Map<String, String> getDbTableColumnMap(List<Column> cols,
			String dbName, String tableName) throws ClassNotFoundException,
			SQLException {

		if (null == cols || 0 == cols.size())
			return null;

		List<DbTableColumn> dbTableColumns = DbUtil.getDbTableColumns(dbName
				+ "." + tableName);

		if (null == dbTableColumns) {
			logger.warn("dbTableColumns is null");
			return null;
		}

		if (dbTableColumns.size() != cols.size()) {
			logger.warn("dbTableColumns.size() is not equal to cols.size()");
			return null;
		}

		Map<String, String> map = new HashMap<String, String>();

		for (int i = 0; i < dbTableColumns.size(); i++) {
			if (null == cols.get(i).getValue()) {
				map.put(dbTableColumns.get(i).getName(), null);
			} else {
				map.put(dbTableColumns.get(i).getName(), cols.get(i).toString());
			}
		}

		return map;
	}

	private static String DB_TABLE;

	@Value("${monitor.db.table}")
	private void setDbTable(String db_table) {
		DB_TABLE = db_table;
	}

	private static String DB_TOPIC;

	@Value("${monitor.db.topic}")
	private void setDbTopic(String db_table) {
		DB_TOPIC = db_table;
	}

	// /**
	// *
	// * @param db
	// * @param table
	// * @return
	// */
	// private static boolean checkExist(String db, String table) {
	// String s = ",t_t.s_user,t_t.s_role,t_t.*,";
	//
	// if (-1 < s.indexOf("," + db + ".*,"))
	// return true;
	//
	// return (-1 < s.indexOf("," + db + "." + table + ","));
	// }
	//
	// public static void main(String[] args) {
	// System.err.println(checkExist("t_t", "s_role1"));
	// }

	/**
	 *
	 * @param db
	 * @param table
	 * @return
	 */
	private boolean checkExist(String db, String table) {

		if (-1 < DB_TABLE.indexOf("," + db + ".*,"))
			return true;

		return (-1 < DB_TABLE.indexOf("," + db + "." + table + ","));
	}

	/**
	 *
	 * @param db
	 * @param table
	 * @param empty
	 * @return 主题名称
	 */
	private String checkExist(String db, String table, Void empty) {
		String s1 = "," + db + ".*:";

		int i = DB_TOPIC.indexOf(s1);

		if (-1 < i) {
			int j = DB_TOPIC.indexOf(",", i + s1.length());
			return DB_TOPIC.substring(i + s1.length(), j);
		}

		s1 = "," + db + "." + table + ":";

		i = DB_TOPIC.indexOf(s1);

		if (-1 < i) {
			int j = DB_TOPIC.indexOf(",", i + s1.length());
			return DB_TOPIC.substring(i + s1.length(), j);
		}

		return null;
	}

}
