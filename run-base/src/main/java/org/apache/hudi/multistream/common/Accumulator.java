package org.apache.hudi.multistream.common;

public enum  Accumulator {

    INSTANCE;

    private String currentTopic = "";

    private long successTasks = 0L;

    private long failedTasks = 0L;

    public String getCurrentTopic() {
        return currentTopic;
    }

    public long getSuccessTasks() {
        return successTasks;
    }

    public long getFailedTasks() {
        return failedTasks;
    }

    public void rebuildCurrentTopic(String newTopic) {
        currentTopic = newTopic;
    }

    public void success() {
        successTasks ++;
    }

    public void failed() {
        failedTasks ++;
    }

}
