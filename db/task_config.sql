CREATE TABLE stream_task
(
    id          bigint(20) NOT NULL AUTO_INCREMENT,
    name        varchar(100) DEFAULT NULL,
    topic       varchar(100) DEFAULT NULL,
    create_time datetime     DEFAULT NOW(),
    create_user varchar(50)  DEFAULT 'machine',
    update_time datetime     DEFAULT NULL,
    update_user varchar(50)  DEFAULT 'machine',
    del_flag    tinyint(1)   DEFAULT 0,
    PRIMARY KEY (id),
    KEY stream_task_name (name),
    KEY stream_task_topic (topic)
);

CREATE TABLE stream_task_config
(
    id          bigint(20)   NOT NULL AUTO_INCREMENT,
    task_id     bigint(20)   NOT NULL,
    type        char(2)     DEFAULT NULL COMMENT '01 delta, 02 properties',
    prop_key    varchar(100) NOT NULL,
    prop_value  varchar(500) NOT NULL,
    create_time datetime    DEFAULT NOW(),
    create_user varchar(50) DEFAULT 'machine',
    update_time datetime    DEFAULT NULL,
    update_user varchar(50) DEFAULT 'machine',
    del_flag    tinyint(1)  DEFAULT 0,
    PRIMARY KEY (id),
    KEY stream_task_config_task_id (task_id)
);
