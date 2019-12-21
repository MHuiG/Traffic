package pre.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * longitude   经度
 * latitude     纬度
 * laci         基站信息
 */
public class Local implements Writable {
    private String longitude;
    private String latitude;
    private String laci;

    public void setLocal(String longitude, String latitude, String laci) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.laci = laci;
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

    public String getLaci() {
        return laci;
    }

    public void setLaci(String laci) {
        this.laci = laci;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getLongitude());
        stringBuilder.append("\001").append(this.getLatitude());
        stringBuilder.append("\001").append(this.getLaci());
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.longitude);
        out.writeUTF(null == latitude ? "" : latitude);
        out.writeUTF(null == laci ? "" : laci);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.longitude = in.readUTF();
        this.latitude = in.readUTF();
        this.laci = in.readUTF();
    }
}
