package pre.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * timestamp 信息记录开始时间
 * imsi 用户识别码（唯一id）
 * lacId 基站位置区编码
 * cellId 扇区编号
 */
public class Signal implements Writable {
    private String timestamp;
    private String imsi;
    private String lacId;
    private String cellId;

    public void setSignal(String timestamp, String imsi, String lacId, String cellId) {
        this.timestamp = timestamp;
        this.imsi = imsi;
        this.lacId = lacId;
        this.cellId = cellId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getLacId() {
        return lacId;
    }

    public void setLacId(String lacId) {
        this.lacId = lacId;
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getTimestamp());
        stringBuilder.append("\001").append(this.getImsi());
        stringBuilder.append("\001").append(this.getLacId());
        stringBuilder.append("\001").append(this.getCellId());
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.timestamp);
        out.writeUTF(null == imsi ? "" : imsi);
        out.writeUTF(null == lacId ? "" : lacId);
        out.writeUTF(null == cellId ? "" : cellId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.timestamp = in.readUTF();
        this.imsi = in.readUTF();
        this.lacId = in.readUTF();
        this.cellId = in.readUTF();
    }
}
