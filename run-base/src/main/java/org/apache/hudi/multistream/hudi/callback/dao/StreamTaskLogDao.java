package org.apache.hudi.multistream.hudi.callback.dao;

import org.apache.hudi.multistream.common.pojo.StreamTaskLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StreamTaskLogDao extends JpaRepository<StreamTaskLog, Long> {
}
