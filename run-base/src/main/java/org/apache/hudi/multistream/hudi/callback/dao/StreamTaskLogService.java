package org.apache.hudi.multistream.hudi.callback.dao;


import org.apache.hudi.multistream.common.pojo.StreamTaskLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("streamTaskLogService")
public class StreamTaskLogService {

    @Autowired
    private StreamTaskLogDao dao;

    public void save(StreamTaskLog streamTaskLog) {
        dao.save(streamTaskLog);
    }
}
