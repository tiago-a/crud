package crud.hbase.model;

import javax.xml.bind.annotation.XmlElement;
import java.io.Serializable;

public class XmlProperty implements Serializable {

    private static final long serialVersionUID = -1L;
    private String name;
    private String value;

    @XmlElement
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @XmlElement
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "XmlProperty{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
