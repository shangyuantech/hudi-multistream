package org.apache.hudi.multistream.hudi.db.dao.task;

import org.apache.hudi.multistream.common.pojo.StreamTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StreamTaskDao extends JpaRepository<StreamTask, Long> {

    @Query(value = "select * from stream_task where topic = ?1 and del_flag = 0", nativeQuery = true)
    List<StreamTask> findByTopic(String topic);

    @Query(value = "select * from stream_task where topic in (:topics) and del_flag = 0", nativeQuery = true)
    List<StreamTask> findByTopics(@Param("topics") List<String> topics);

    List<StreamTask> findByName(String name);

    void deleteByName(String name);
}
