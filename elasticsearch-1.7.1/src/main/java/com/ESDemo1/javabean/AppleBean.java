package com.ESDemo1.javabean;

/**
 * Created by wxmimperio on 2015/9/25.
 */
public class AppleBean {
    private String color;
    private int size;
    private float price;

    public AppleBean(String color, int size, float price) {
        this.color = color;
        this.size = size;
        this.price = price;
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

    public float getprice() {
        return price;
    }

    public void setprice(float price) {
        this.price = price;
    }
}
