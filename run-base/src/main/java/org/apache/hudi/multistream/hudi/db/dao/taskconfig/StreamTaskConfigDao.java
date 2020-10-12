package org.apache.hudi.multistream.hudi.db.dao.taskconfig;

import org.apache.hudi.multistream.common.pojo.StreamTaskConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StreamTaskConfigDao extends JpaRepository<StreamTaskConfig, Long> {

    @Query(value = "select * from stream_task_config where task_id = ?1 and del_flag = 0", nativeQuery = true)
    List<StreamTaskConfig> findByTaskId(Long taskId);

    void deleteByTaskId(Long taskId);
}
