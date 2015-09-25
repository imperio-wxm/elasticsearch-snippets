package com.ESDemo1.javabean;

/**
 * Created by wxmimperio on 2015/9/25.
 */
public class AppleBean {
    private String color;
    private int size;
    private float prize;

    public AppleBean(String color, int size, float prize) {
        this.color = color;
        this.size = size;
        this.prize = prize;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public float getPrize() {
        return prize;
    }

    public void setPrize(float prize) {
        this.prize = prize;
    }
}
