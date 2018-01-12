package com.minedata.springLearn;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.minedata.springLearn.entity.Computer;
import com.minedata.springLearn.entity.Person;
import com.minedata.springLearn.entity.Tecnology;

@Configuration
@ComponentScan(basePackageClasses = Tecnology.class)
public class PersonConfig {

    @Bean(name = "computer")
    public Tecnology getComputer() {
        return new Computer();
    }

    @Bean
    @Autowired
    public Person getPerson(Tecnology computer) {
        return new Person(computer);
    }


}
