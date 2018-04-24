package net.foreworld.yx.model;

/**
 *
 * @author huangxin
 *
 */
public class BinlogMasterStatus {

	private String file_name;
	private Long position;

	public String getFile_name() {
		return file_name;
	}

	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	public Long getPosition() {
		return position;
	}

	public void setPosition(Long position) {
		this.position = position;
	}

}
