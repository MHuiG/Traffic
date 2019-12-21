package pre.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * longitude   经度
 * latitude     纬度
 * lacId 基站位置区编码
 * cellId 扇区编号
 */
public class Local implements Writable {
    private String longitude;
    private String latitude;
    private String lacId;
    private String cellId;

    public void setLocal(String longitude, String latitude, String lacId, String cellId) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.lacId = lacId;
        this.cellId = cellId;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
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
        stringBuilder.append(this.getLongitude());
        stringBuilder.append("\001").append(this.getLatitude());
        stringBuilder.append("\001").append(this.getLacId());
        stringBuilder.append("\001").append(this.getCellId());
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.longitude);
        out.writeUTF(null == latitude ? "" : latitude);
        out.writeUTF(null == lacId ? "" : lacId);
        out.writeUTF(null == cellId ? "" : cellId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.longitude = in.readUTF();
        this.latitude = in.readUTF();
        this.lacId = in.readUTF();
        this.cellId = in.readUTF();
    }
}
