package com.ibeifeng.sparkproject.domain;

/**
 * @Description: TODO
 * @Author: wanghailin
 * @Date: 2019/4/17
 */
public class SessionRandomExtract {

    private long taskId;
    private String sessionId;
    private String startTime;
    private String endTime;
    private String searchKeywords;
    private String clickCategoryIds;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getSearchKeywords() {
        return searchKeywords;
    }

    public void setSearchKeywords(String searchKeywords) {
        this.searchKeywords = searchKeywords;
    }

    public String getClickCategoryIds() {
        return clickCategoryIds;
    }

    public void setClickCategoryIds(String clickCategoryIds) {
        this.clickCategoryIds = clickCategoryIds;
    }
}
