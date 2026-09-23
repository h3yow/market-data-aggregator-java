package com.github.h3yow.marketdataaggregatorjava.model;

/**
 * 行情接入的可区分处理结果。
 */
public class IngestResult {
    private boolean accepted;
    private IngestDecision decision;
    private String reason;
    private Long windowStart;
    private Long changeSeq;

    public IngestResult() {
    }

    public boolean isAccepted() { return accepted; }
    public void setAccepted(boolean accepted) { this.accepted = accepted; }
    public IngestDecision getDecision() { return decision; }
    public void setDecision(IngestDecision decision) { this.decision = decision; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public Long getWindowStart() { return windowStart; }
    public void setWindowStart(Long windowStart) { this.windowStart = windowStart; }
    public Long getChangeSeq() { return changeSeq; }
    public void setChangeSeq(Long changeSeq) { this.changeSeq = changeSeq; }
}
