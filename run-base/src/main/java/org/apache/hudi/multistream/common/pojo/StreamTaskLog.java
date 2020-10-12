package org.apache.hudi.multistream.common.pojo;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Entity
@Table(name = "stream_task_log")
public class StreamTaskLog implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "from_topic")
    private String fromTopic;

    @Column(name = "to_source")
    private String toSource;

    @Column(name = "to_table")
    private String toTable;

    @Column(name = "commit_time")
    private String commitTime;

    @Column(name = "base_path")
    private String basePath;

    @Column(name = "hudi_table")
    private String hudiTable;

    @Column(name = "insert_rows")
    private Long insertRows;

    @Column(name = "delete_rows")
    private Long deleteRows;

    @Column(name = "update_rows")
    private Long updateRows;

    @CreatedDate
    @Column(name = "create_time")
    private Date createTime;

    @Column(name = "create_user")
    private String createUser;

    @LastModifiedDate
    @Column(name = "update_time")
    private Date updateTime;

    @Column(name = "update_user")
    private String updateUser;

    @Column(name = "del_flag")
    private Boolean delFlag;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFromTopic() {
        return fromTopic;
    }

    public void setFromTopic(String fromTopic) {
        this.fromTopic = fromTopic;
    }

    public String getToSource() {
        return toSource;
    }

    public void setToSource(String toSource) {
        this.toSource = toSource;
    }

    public String getToTable() {
        return toTable;
    }

    public void setToTable(String toTable) {
        this.toTable = toTable;
    }

    public String getCommitTime() {
        return commitTime;
    }

    public void setCommitTime(String commitTime) {
        this.commitTime = commitTime;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getHudiTable() {
        return hudiTable;
    }

    public void setHudiTable(String hudiTable) {
        this.hudiTable = hudiTable;
    }

    public Long getInsertRows() {
        return insertRows;
    }

    public void setInsertRows(Long insertRows) {
        this.insertRows = insertRows;
    }

    public Long getDeleteRows() {
        return deleteRows;
    }

    public void setDeleteRows(Long deleteRows) {
        this.deleteRows = deleteRows;
    }

    public Long getUpdateRows() {
        return updateRows;
    }

    public void setUpdateRows(Long updateRows) {
        this.updateRows = updateRows;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getUpdateUser() {
        return updateUser;
    }

    public void setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
    }

    public Boolean getDelFlag() {
        return delFlag;
    }

    public void setDelFlag(Boolean delFlag) {
        this.delFlag = delFlag;
    }

    @Override
    public String toString() {
        return "StreamTaskLog{" +
                "id=" + id +
                ", fromTopic='" + fromTopic + '\'' +
                ", toSource='" + toSource + '\'' +
                ", toTable='" + toTable + '\'' +
                ", commitTime='" + commitTime + '\'' +
                ", basePath='" + basePath + '\'' +
                ", hudiTable='" + hudiTable + '\'' +
                ", insertRows=" + insertRows +
                ", deleteRows=" + deleteRows +
                ", updateRows=" + updateRows +
                ", createTime=" + createTime +
                ", createUser='" + createUser + '\'' +
                ", updateTime=" + updateTime +
                ", updateUser='" + updateUser + '\'' +
                ", delFlag=" + delFlag +
                '}';
    }
}
