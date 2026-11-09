package com.lab.evaluation26;

public final class ReportMsg extends Msg {
    private final boolean reset;

    public ReportMsg(boolean reset) { this.reset = reset; }
    public boolean isReset() { return reset; }

    @Override
    public String toString() { return "ReportMsg(reset=" + reset + ")"; }
}
