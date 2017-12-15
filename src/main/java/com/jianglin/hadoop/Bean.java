package com.jianglin.hadoop;

import javax.xml.bind.annotation.*;
import java.io.Serializable;

/**
 * Created by admin on 2017/12/5.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
@XmlType(name = "bean", propOrder = { "aa", "bb", "cc", "id" })
public class Bean implements Serializable {
    @XmlElement(required = true)
    private String id;
    @XmlElement
    private String aa;
    @XmlElement
    private String bb;
    @XmlElement
    private String cc;

    public String getAa() {
        return aa;
    }

    public void setAa(String aa) {
        this.aa = aa;
    }

    public String getBb() {
        return bb;
    }

    public void setBb(String bb) {
        this.bb = bb;
    }

    public String getCc() {
        return cc;
    }

    public void setCc(String cc) {
        this.cc = cc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
