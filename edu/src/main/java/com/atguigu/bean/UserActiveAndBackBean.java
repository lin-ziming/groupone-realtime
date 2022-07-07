package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ClassName UserActiveAndBackBean
 * @Author Chris
 * @Description TODO
 * @Date 2022/7/6 18:57
 **/

@Data
@AllArgsConstructor
public class UserActiveAndBackBean {
	// 窗口起始时间
	String stt;

	// 窗口终止时间
	String edt;

	// 回流用户数
	Long backCt;

	// 活跃用户数
	Long ActiveCt;

	// 时间戳
	Long ts;
}
