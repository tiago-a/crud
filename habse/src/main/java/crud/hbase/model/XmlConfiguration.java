package crud.hbase.model;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "configuration")
public final class XmlConfiguration implements Serializable {

    private static final long serialVersionUID = -1L;
    private List<XmlProperty> properties;

    @XmlElement(name = "property")
    public List<XmlProperty> getProperties() {
        return properties;
    }

    public void setProperties(List<XmlProperty> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "XmlConfiguration{" +
                "properties=" + properties +
                '}';
    }
}
