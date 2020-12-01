package com.anand.techservices.domain;


import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode

public class CustomerEntity implements Serializable {
    Integer id;
    String name;
    String lastName;
    String city;

    public String toString() {
        StringBuffer sbf = new StringBuffer();
        sbf.append(id).append(",").append(name).append(",")
                .append(lastName).append(",").append(city);
        return sbf.toString();
    }
}
