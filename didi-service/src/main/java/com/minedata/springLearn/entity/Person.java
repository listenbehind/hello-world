package com.minedata.springLearn.entity;


public class Person implements Creatures {
    private String name;
    private String age;
    public Tecnology computer;

    public Person(String name, String age) {
        super();
        this.name = name;
        this.age = age;
    }

    public Person(Tecnology computer) {
        this.computer = computer;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }


    @Override
    public void run() {
        System.out.println("PEOPLE RUN!");

    }
}
