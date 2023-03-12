package com.example.checkout;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;


@Setter
@Getter
@ToString
public class CheckoutDto {

    private Long checkOutId;
    private Long memberId;
    private Long productId;
    private Long amount;
    private String shippingAddress;
    private Date createdAt;
}
