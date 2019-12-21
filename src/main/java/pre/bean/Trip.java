package pre.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * longitude   经度
 * latitude  纬度
 * mode      方式（数据中包括地铁、公交）
 * modeName 站名
 * modeNum  线路
 */
public class Trip implements Writable {
    private String longitude;
    private String latitude;
    private String mode;
    private String modeName;
    private String modeNum;

    public void setTrip(String longitude, String latitude, String mode, String modeName, String modeNum) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.mode = mode;
        this.modeName = modeName;
        this.modeNum = modeNum;
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

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getModeName() {
        return modeName;
    }

    public void setModeName(String modeName) {
        this.modeName = modeName;
    }

    public String getModeNum() {
        return modeNum;
    }

    public void setModeNum(String modeNum) {
        this.modeNum = modeNum;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getLongitude());
        stringBuilder.append("\001").append(this.getLatitude());
        stringBuilder.append("\001").append(this.getMode());
        stringBuilder.append("\001").append(this.getModeName());
        stringBuilder.append("\001").append(this.getModeNum());
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.longitude);
        out.writeUTF(null == latitude ? "" : latitude);
        out.writeUTF(null == mode ? "" : mode);
        out.writeUTF(null == modeName ? "" : modeName);
        out.writeUTF(null == modeNum ? "" : modeNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.longitude = in.readUTF();
        this.latitude = in.readUTF();
        this.mode = in.readUTF();
        this.modeName = in.readUTF();
        this.modeNum = in.readUTF();
    }
}
