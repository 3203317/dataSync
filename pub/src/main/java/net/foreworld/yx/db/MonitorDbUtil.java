package net.foreworld.yx.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.foreworld.yx.db.model.BinlogMasterStatus;
import net.foreworld.yx.db.model.SendData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 *
 * @author huangxin
 *
 */
@Component
public final class MonitorDbUtil {

	private static String DRIVERCLASS;

	@Value("${monitor.jdbc.driverClass}")
	private void setDriverClass(String driverClass) {
		DRIVERCLASS = driverClass;
	}

	private static String HOST;

	@Value("${monitor.jdbc.url}")
	private void setHost(String host) {
		HOST = host;
	}

	private static String USER;

	@Value("${monitor.jdbc.user}")
	private void setUser(String user) {
		USER = user;
	}

	private static String PASS;

	@Value("${monitor.jdbc.password}")
	private void setPass(String pass) {
		PASS = pass;
	}

	private static final Logger logger = LoggerFactory
			.getLogger(MonitorDbUtil.class);

	/**
	 *
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static Connection getConn() throws ClassNotFoundException,
			SQLException {
		Class.forName(DRIVERCLASS);
		return DriverManager.getConnection(HOST, USER, PASS);
	}

	/**
	 *
	 * @param v
	 * @return
	 */
	public static SendData getSql(Void v) {
		Connection conn = null;
		Statement stat = null;
		ResultSet rs = null;

		SendData sd = null;

		try {
			conn = getConn();
			stat = conn.createStatement();
			rs = stat.executeQuery(NUM);

			HashMap<String, List<String>> map = new HashMap<String, List<String>>();

			long _id = 0;

			while (rs.next()) {

				List<String> _val = map.get(rs.getString("topic_name"));

				if (null == _val) {
					_val = new ArrayList<String>();
					map.put(rs.getString("topic_name"), _val);
				}

				_val.add(rs.getString("s_q_l"));

				if (rs.isLast())
					_id = rs.getLong("id");
			}

			if (0 < map.size()) {
				sd = new SendData();
				sd.setSql(gson.toJson(map));
				sd.setPos(_id);
			}

		} catch (SQLException | ClassNotFoundException e) {
			logger.error(e.getMessage(), e);

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

		return sd;
	}

	private static String NUM;

	@Value("SELECT * FROM s_sql WHERE id>(SELECT _value FROM s_cfg WHERE _key='sql_position_id') ORDER BY id ASC LIMIT ${publish.num}")
	private void setNum(String num) {
		NUM = num;
	}

	/**
	 *
	 * @return
	 */
	public static SendData getSql() {
		Connection conn = null;
		Statement stat = null;
		ResultSet rs = null;

		SendData sd = null;

		try {
			conn = getConn();
			stat = conn.createStatement();
			rs = stat.executeQuery(NUM);

			long _id = 0;
			String _str = "";

			while (rs.next()) {
				_str += rs.getString("s_q_l");

				if (rs.isLast())
					_id = rs.getLong("id");
				else
					_str += ",";
			}

			if (!"".equals(_str)) {
				sd = new SendData();
				sd.setSql("[" + _str + "]");
				sd.setPos(_id);
			}

		} catch (SQLException | ClassNotFoundException e) {
			logger.error(e.getMessage(), e);

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

		return sd;
	}

	/**
	 *
	 */
	public static void test_setSql() {
		Connection conn = null;
		PreparedStatement ps1 = null;

		try {
			conn = getConn();

			ps1 = conn.prepareStatement("INSERT INTO h222 (sdf) VALUES (?)");

			for (int i = 1; i < 10001; i++) {
				ps1.setObject(1, i);
				ps1.execute();
			}

		} catch (SQLException | ClassNotFoundException e) {
			logger.error(e.getMessage(), e);
			System.exit(1);

		} finally {

			if (null != ps1) {
				try {
					ps1.close();
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
	}

	/**
	 *
	 * @param sql
	 * @param topic_name
	 */
	public static void setSql(String sql, String topic_name) {
		Connection conn = null;
		PreparedStatement ps1 = null;

		try {
			conn = getConn();

			ps1 = conn
					.prepareStatement("INSERT INTO s_sql (s_q_l, topic_name) VALUES (?, ?)");
			ps1.setObject(1, sql);
			ps1.setObject(2, topic_name);
			ps1.execute();

		} catch (SQLException | ClassNotFoundException e) {
			logger.error(sql);
			System.exit(1);

		} finally {

			if (null != ps1) {
				try {
					ps1.close();
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
	}

	/**
	 *
	 * @param sql
	 */
	public static void setSql(String sql) {
		Connection conn = null;
		PreparedStatement ps1 = null;

		try {
			conn = getConn();

			ps1 = conn.prepareStatement("INSERT INTO s_sql (s_q_l) VALUES (?)");
			ps1.setObject(1, sql);
			ps1.execute();

		} catch (SQLException | ClassNotFoundException e) {
			logger.error(sql);
			System.exit(1);

		} finally {

			if (null != ps1) {
				try {
					ps1.close();
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
	}

	/**
	 *
	 * @param pos
	 */
	public static void setCfgPos(long pos) {
		Connection conn = null;
		PreparedStatement ps1 = null;

		try {
			conn = getConn();

			ps1 = conn.prepareStatement(sql);
			ps1.setObject(1, pos);
			ps1.setObject(2, "sql_position_id");
			ps1.execute();

		} catch (SQLException | ClassNotFoundException e) {
			logger.error(e.getMessage(), e);

		} finally {

			if (null != ps1) {
				try {
					ps1.close();
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

	}

	static String sql = "UPDATE s_cfg SET _value=? WHERE _key=?";

	/**
	 *
	 * @param binlog_file_name
	 * @param binlog_position
	 */
	public static void setCfgBinlog(String binlog_file_name,
			String binlog_position) {
		Connection conn = null;
		PreparedStatement ps1 = null;
		PreparedStatement ps2 = null;

		try {
			conn = getConn();
			conn.setAutoCommit(false);

			ps1 = conn.prepareStatement(sql);
			ps1.setObject(1, binlog_file_name);
			ps1.setObject(2, "binlog_file_name");
			ps1.execute();

			ps2 = conn.prepareStatement(sql);
			ps2.setObject(1, binlog_position);
			ps2.setObject(2, "binlog_position");
			ps2.execute();

			conn.commit();

		} catch (SQLException | ClassNotFoundException e) {
			logger.error(e.getMessage(), e);

			try {
				conn.rollback();
			} catch (SQLException e1) {
				logger.error(e.getMessage(), e1);
			}

		} finally {

			if (null != ps1) {
				try {
					ps1.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}

			if (null != ps2) {
				try {
					ps2.close();
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
	}

	/**
	 *
	 */
	public static void setCfgBinlog() {
		BinlogMasterStatus b = DbUtil.getBinlogMasterStatus();
		setCfgBinlog(b.getFile_name(), b.getPosition().toString());
	}

	/**
	 *
	 * @return
	 */
	public static BinlogMasterStatus getBinlogMasterStatus() {

		BinlogMasterStatus binlogMasterStatus = null;

		Connection conn = null;
		Statement stat = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			stat = conn.createStatement();
			rs = stat.executeQuery("SELECT * FROM s_cfg");

			binlogMasterStatus = new BinlogMasterStatus();

			while (rs.next()) {

				if ("binlog_file_name".equals(rs.getString("_key"))) {
					binlogMasterStatus.setFile_name(rs.getString("_value"));
				} else if ("binlog_position".equals(rs.getString("_key"))) {
					binlogMasterStatus.setPosition(rs.getLong("_value"));
				}

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

	public static Gson gson = new GsonBuilder().serializeNulls().create();

	public static void main(String[] args) {

		HashMap<String, List<String>> map = new HashMap<String, List<String>>();

		List<String> l = new ArrayList<String>();
		l.add("a1");
		l.add("a2");

		map.put("a", l);

		System.err.println(gson.toJson(map));
		System.err.println(map.size());

		List<String> l2 = map.get("a");
		l2.add("a3");

		System.err.println(gson.toJson(map));
		System.err.println(map.size());

	}

}
