package com.minedata.springLearn;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.minedata.springLearn.entity.Computer;
import com.minedata.springLearn.entity.Person;


public class TestSpring {
    public static void main(String[] args) {
        ApplicationContext ctx = new AnnotationConfigApplicationContext(PersonConfig.class);
        Person bean = ctx.getBean(Person.class);
        bean.computer.programming();
        Computer bean2 = (Computer) ctx.getBean("computer");
        System.out.println(bean2 == bean.computer);
    }
}
