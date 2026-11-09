package com.lab.evaluation26;

public final class RecordMsg extends Msg {
    private final int key;
    private final int value;

    public RecordMsg(int key, int value) {
        this.key = key;
        this.value = value;
    }
    public int getKey() { return key; }
    public int getValue() { return value; }

    @Override
    public String toString() {
        return "RecordMsg(key=" + key + ", value=" + value + ")";
    }
}
