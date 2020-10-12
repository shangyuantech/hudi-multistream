create table stream_task_log
(
    id          bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'pk',
    from_topic  varchar(100) COMMENT 'kafka topic',
    to_source   varchar(100) DEFAULT NULL COMMENT 'to source',
    to_table    varchar(100) DEFAULT NULL COMMENT 'to table',
    commit_time varchar(50)  DEFAULT NULL,
    base_path   varchar(100) DEFAULT NULL COMMENT 'base path',
    hudi_table  varchar(100) DEFAULT NULL COMMENT 'hudi table',
    insert_rows bigint(20)   DEFAULT NULL COMMENT 'insert rows',
    delete_rows bigint(20)   DEFAULT NULL COMMENT 'delete rows',
    update_rows bigint(20)   DEFAULT NULL COMMENT 'update rows',
    create_time datetime     DEFAULT NOW(),
    create_user varchar(50)  DEFAULT 'machine',
    update_time datetime     DEFAULT NULL,
    update_user varchar(50)  DEFAULT 'machine',
    del_flag    tinyint(1)   DEFAULT 0,
    PRIMARY KEY (`id`),
    KEY stream_task_log_topic (from_topic),
    KEY stream_task_log_hudi_table (hudi_table)
);